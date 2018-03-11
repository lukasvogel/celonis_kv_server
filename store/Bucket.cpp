//
// Created by Lukas on 09.03.2018.
//

#include "Bucket.h"

bool Bucket::put(size_t hash, string key, string value) {
    // first delete a potentially already existing entry (for update)
    del(hash, key);

    int64_t free_space = (this->data_begin - this->offset_end);

    size_t key_size = key.size() + 1; // null termination
    size_t value_size = value.size() + 1;
    size_t entry_size = key_size + value_size + sizeof(EntryHeader);

    if (free_space >= entry_size + sizeof(EntryPosition)) {
        // we have enough room, write the data
        insert(hash, key, value);
    } else {
        // not enough space, first try compacting
        cout << "compacting..." << endl;
        cout << "usage before: " << get_usage() << endl;
        compact();
        cout << "usage after: " << get_usage() << endl;

        // retry insertion
        free_space = (this->data_begin - this->offset_end);
        if (free_space >= entry_size + sizeof(EntryPosition)) {
            cout << "enough room for now..." << endl;
            // we have enough room, write the data
            insert(hash, key, value);
        } else {
            // if still not enough space, we can*t do anything
            // caller has to split us
            return false;
        }
    }

    return true;
}

bool Bucket::get(size_t hash, string key, string *result) {

    EntryPosition *ep;
    EntryHeader *eh;

    if (find(hash, key, &ep, &eh)) {
        char *cur_value = this->data + ep->offset + sizeof(EntryHeader) + eh->key_size;
        *result = cur_value;
        return true;
    }
    return false;

}

void Bucket::del(size_t hash, string key) {
    EntryPosition *ep;
    EntryHeader *eh;

    if (find(hash, key, &ep, &eh)) {
        ep->status = EntryPosition::Status::DELETED;
    }
}

bool Bucket::find(size_t hash, string key, EntryPosition **position, EntryHeader **header) {
    // We iterate from the back so we do not look at deleted entries first
    // TODO: possible optimization: If we find a deleted entry for the same key, we know we can abort
    size_t offset = this->offset_end;

    while (offset > 0) {
        auto *ep = reinterpret_cast<EntryPosition *>(this->data + offset - sizeof(EntryPosition));

        if (ep->status == EntryPosition::Status::ACTIVE && hash == ep->hash_code) {
            char *cur_key = this->data + ep->offset + sizeof(EntryHeader);
            if (key == string(cur_key)) {
                //TODO: this does not work if the page gets purged and we keep the reference...
                auto *eh = reinterpret_cast<EntryHeader *>(this->data + ep->offset);
                *position = ep;
                *header = eh;
                return true;
            }

        }
        offset -= sizeof(EntryPosition);
    }
    return false;
}

void Bucket::insert(size_t hash, string key, string value) {
    // calculate all sizes required
    size_t key_size = key.size() + 1; // null termination
    size_t value_size = value.size() + 1;

    // generate the header for the entry
    EntryHeader eh(key_size, value_size);

    // copy the entry over
    memcpy(this->data + this->data_begin - value_size, value.c_str(), value_size);
    this->data_begin -= value_size;
    memcpy(this->data + this->data_begin - key_size, key.c_str(), key_size);
    this->data_begin -= key_size;
    memcpy(this->data + this->data_begin - sizeof(EntryHeader), (char *) &eh, sizeof(EntryHeader));
    this->data_begin -= sizeof(EntryHeader);

    // document the position of the entry
    EntryPosition ep(this->data_begin, hash);
    memcpy(this->data + this->offset_end, &ep, sizeof(EntryPosition));
    this->offset_end += sizeof(EntryPosition);
}

void Bucket::compact() {
    size_t read_ep = 0;
    size_t write_ep = 0;

    size_t write_entry = SIZE;


    while (read_ep < this->offset_end) {
        auto *ep = reinterpret_cast<EntryPosition *>(this->data + read_ep);

        // Ignore deleted entries
        if (ep->status == EntryPosition::Status::DELETED) {
            read_ep += sizeof(EntryPosition);
            continue;
        }


        auto *eh = reinterpret_cast<EntryHeader *>(this->data + ep->offset);
        size_t entry_size = eh->key_size + eh->value_size + sizeof(EntryHeader);

        //if we have a gap, move entry further back
        if (ep->offset + entry_size < write_entry) {
            memmove(this->data + write_entry - entry_size, this->data + ep->offset, entry_size);
        }

        write_entry -= entry_size;
        ep->offset = write_entry;

        //if we have an entry pointer gap, move the entry pointer to the front
        if (write_ep < read_ep) {
            memmove(this->data + write_ep, ep, sizeof(EntryPosition));
        }

        write_ep += sizeof(EntryPosition);
        read_ep += sizeof(EntryPosition);

    }

    this->offset_end = write_ep;
    this->data_begin = write_entry;

}

double Bucket::get_usage() {
    return 1 - (this->data_begin - this->offset_end) / (SIZE * 1.0);
}

void Bucket::split(size_t global_depth, Bucket &new_bucket) {
    size_t ep_offset = 0;

    while (ep_offset < this->offset_end) {
        auto *ep = reinterpret_cast<EntryPosition *>(this->data + ep_offset);
        auto hash_sig_part = ep->hash_code & ((1 << global_depth) - 1);

        if (((hash_sig_part >> local_depth) & 1) == 1) {
            // belongs into the new bucket, insert it there
            auto *eh = reinterpret_cast<EntryHeader *>(this->data + ep->offset);
            char *key = this->data + ep->offset + sizeof(EntryHeader);
            char *value = key + eh->key_size;

            new_bucket.insert(ep->hash_code, string(key), string(value));
            // remove it from this bucket
            ep->status = EntryPosition::Status::DELETED;
        }
        ep_offset += sizeof(EntryPosition);
    }
    // all other entries stay here, compact them
    compact();

    local_depth += 1;
    new_bucket.local_depth = local_depth;
}

char *Bucket::get_data() {
    return data;
}




