//
// Created by Lukas on 09.03.2018.
//

#include <cstring>
#include "Bucket.h"

bool Bucket::put(size_t hash, const string &key, const string &value) {
    // first delete a potentially already existing entry (for update)
    del(hash, key);

    int64_t free_space = (header.data_begin - header.offset_end);
    size_t key_size = key.size() + 1; // null termination
    size_t value_size = value.size() + 1;
    size_t entry_size = key_size + value_size + sizeof(EntryHeader);

    if (free_space >= entry_size + sizeof(EntryPosition)) {
        // we have enough room, write the data
        insert(hash, key, value);
    } else if (contains_deleted_entries){
        // not enough space, but we can make space! First try compacting
        compact();

        // retry insertion
        free_space = (header.data_begin - header.offset_end);
        if (free_space >= entry_size + sizeof(EntryPosition)) {
            insert(hash, key, value);
        } else {
            // if still not enough space, we can*t do anything
            // caller has to split us
            return false;
        }
    } else {
        return false;
    }
    return true;
}

bool Bucket::get(size_t hash, const string &key, string &result) {
    EntryPosition *ep;
    EntryHeader *eh;

    if (find(hash, key, ep, eh)) {
        char *cur_value = data + ep->offset + sizeof(EntryHeader) + eh->key_size;
        result = cur_value;
        return true;
    }
    return false;

}

void Bucket::del(size_t hash, const string &key) {
    EntryPosition *ep;
    EntryHeader *eh;

    if (find(hash, key, ep, eh)) {
        ep->status = EntryPosition::Status::DELETED;
        contains_deleted_entries = true;
    }
}

bool Bucket::find(size_t hash, const string &key, EntryPosition *&position, EntryHeader *&entry_header) {
    // We iterate from the back so we do not look at deleted entries first
    size_t offset = header.offset_end;

    while (offset > 0) {
        auto *ep = reinterpret_cast<EntryPosition *>(data + offset - sizeof(EntryPosition));

        if (hash == ep->hash_code) {
            char *cur_key = data + ep->offset + sizeof(EntryHeader);

            if (key == string(cur_key)) {
                //if the entry is active, parse its contents and return them
                if (ep->status == EntryPosition::Status::ACTIVE) {
                    auto *eh = reinterpret_cast<EntryHeader *>(data + ep->offset);
                    position = ep;
                    entry_header = eh;
                    return true;
                }
                // if the entry is deleted, we can return as we've scanned from the back and would have hit an
                // updated value already
                if (ep->status == EntryPosition::Status::DELETED) {
                    return false;
                }
            }
        }
        offset -= sizeof(EntryPosition);
    }
    return false;
}

void Bucket::insert(size_t hash, const string &key, const string &value) {
    // calculate all sizes required
    size_t key_size = key.size() + 1; // null termination
    size_t value_size = value.size() + 1;

    // generate the header for the entry
    EntryHeader eh(key_size, value_size);

    // copy the entry over
    memcpy(data + header.data_begin - value_size, value.c_str(), value_size);
    header.data_begin -= value_size;
    memcpy(data + header.data_begin - key_size, key.c_str(), key_size);
    header.data_begin -= key_size;
    memcpy(data + header.data_begin - sizeof(EntryHeader), (char *) &eh, sizeof(EntryHeader));
    header.data_begin -= sizeof(EntryHeader);

    // document the position of the entry
    EntryPosition ep(header.data_begin, hash);
    memcpy(data + header.offset_end, &ep, sizeof(EntryPosition));
    header.offset_end += sizeof(EntryPosition);
}

void Bucket::compact() {
    size_t read_ep = 0;
    size_t write_ep = 0;

    size_t write_entry = BUCKET_SIZE;

    while (read_ep < header.offset_end) {
        auto *ep = reinterpret_cast<EntryPosition *>(data + read_ep);

        // Ignore deleted entries
        if (ep->status == EntryPosition::Status::DELETED) {
            read_ep += sizeof(EntryPosition);
            continue;
        }

        // Read the entry
        auto *eh = reinterpret_cast<EntryHeader *>(data + ep->offset);
        size_t entry_size = eh->key_size + eh->value_size + sizeof(EntryHeader);

        //if we have a gap, move entry further back
        if (ep->offset + entry_size < write_entry) {
            memmove(data + write_entry - entry_size, data + ep->offset, entry_size);
        }
        write_entry -= entry_size;
        ep->offset = write_entry;

        //if we have an entry pointer gap, move the entry pointer
        if (write_ep < read_ep) {
            memmove(data + write_ep, ep, sizeof(EntryPosition));
        }
        write_ep += sizeof(EntryPosition);
        read_ep += sizeof(EntryPosition);
    }
    // save new offsets
    header.offset_end = write_ep;
    header.data_begin = write_entry;
    contains_deleted_entries = false;
}

double Bucket::get_usage() {
    return 1 - (header.data_begin - header.offset_end) / (BUCKET_SIZE * 1.0);
}

void Bucket::split(size_t global_depth, Bucket &new_bucket, size_t hash_to_insert, string key_to_insert, string value_to_insert) {
    size_t ep_offset = 0;
    // Iterate through all entries
    while (ep_offset < header.offset_end) {
        auto *ep = reinterpret_cast<EntryPosition *>(data + ep_offset);
        auto hash_sig_part = ep->hash_code & ((1 << global_depth) - 1);

        // If hash has a 1 at the local depth, it belongs into the new bucket, insert it there
        if (((hash_sig_part >> header.local_depth) & 1) == 1) {
            auto *eh = reinterpret_cast<EntryHeader *>(data + ep->offset);
            char *key = data + ep->offset + sizeof(EntryHeader);
            char *value = key + eh->key_size;
            new_bucket.insert(ep->hash_code, string(key), string(value));

            // remove it from this bucket, compaction will take care of it later on
            ep->status = EntryPosition::Status::DELETED;
        }
        ep_offset += sizeof(EntryPosition);
    }

    // all other entries stay here, compact them
    compact();
    auto hash_sig_part = hash_to_insert & ((1 << global_depth) - 1);
    if (((hash_sig_part >> header.local_depth) & 1) == 1) {
        new_bucket.insert(hash_to_insert, key_to_insert, value_to_insert);
    } else {
        insert(hash_to_insert, key_to_insert, value_to_insert);
    }


    // update depths
    header.local_depth += 1;
    new_bucket.header.local_depth = header.local_depth;

}

char *Bucket::get_data() {
    return data;
}