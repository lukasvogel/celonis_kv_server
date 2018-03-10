//
// Created by Lukas on 09.03.2018.
//

#include "Buffer.h"
#include "EntryHeader.h"

void Buffer::put(size_t hash, string key, string value) {
    // first delete a potentially already existing entry (for update)
    del(hash, key);

    int64_t free_space = (data_begin - offset_end);

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
        free_space = (data_begin - offset_end);
        if (free_space >= entry_size + sizeof(EntryPosition)) {
            // we have enough room, write the data
            insert(hash, key, value);
        } else {
            // if still not enough space, split
            cout << "todo: split" << endl;
            //TODO: split
        }
    }

    //cout << "usage: " << get_usage() << endl;
}

bool Buffer::get(size_t hash, string key, string *result) {

    EntryPosition *ep;
    EntryHeader *eh;

    if (find(hash, key, &ep, &eh)) {
        char *cur_value = data + ep->offset + sizeof(EntryHeader) + eh->key_size;
        *result = cur_value;
        return true;
    }
    return false;

}

void Buffer::del(size_t hash, string key) {
    EntryPosition *ep;
    EntryHeader *eh;

    if (find(hash, key, &ep, &eh)) {
        cout << "deleting entry: " << key << endl;
        ep->status = EntryPosition::Status::DELETED;
    }
}

bool Buffer::find(size_t hash, string key, EntryPosition **position, EntryHeader **header) {
    // We iterate from the back so we do not look at deleted entries first
    size_t offset = offset_end;

    while (offset > 0) {
        auto *ep = reinterpret_cast<EntryPosition *>(data + offset - sizeof(EntryPosition));

        if (ep->status == EntryPosition::Status::ACTIVE && hash == ep->hash_code) {
            char *cur_key = data + ep->offset + sizeof(EntryHeader);
            if (key == string(cur_key)) {
                auto *eh = reinterpret_cast<EntryHeader *>(data + ep->offset);
                *position = ep;
                *header = eh;
                return true;
            }

        }
        offset -= sizeof(EntryPosition);
    }
    return false;
}

void Buffer::insert(size_t hash, string key, string value) {
    // calculate all sizes required
    size_t key_size = key.size() + 1; // null termination
    size_t value_size = value.size() + 1;

    // generate the header for the entry
    EntryHeader eh(key_size, value_size);

    // copy over the entry
    memcpy(data + data_begin - value_size, value.c_str(), value_size);
    data_begin -= value_size;
    memcpy(data + data_begin - key_size, key.c_str(), key_size);
    data_begin -= key_size;
    memcpy(data + data_begin - sizeof(EntryHeader), (char *) &eh, sizeof(EntryHeader));
    data_begin -= sizeof(EntryHeader);

    // document the position of the entry
    EntryPosition ep(data_begin, hash);
    memcpy(data + offset_end, &ep, sizeof(EntryPosition));
    offset_end += sizeof(EntryPosition);
}

void Buffer::compact() {
    size_t read_ep = 0;
    size_t write_ep = 0;

    size_t write_entry = SIZE;


    while (read_ep < offset_end) {
        auto *ep = reinterpret_cast<EntryPosition *>(data + read_ep);

        // Ignore deleted entries
        if (ep->status == EntryPosition::Status::DELETED) {
            read_ep += sizeof(EntryPosition);
            continue;
        }


        auto *eh = reinterpret_cast<EntryHeader *>(data + ep->offset);
        size_t entry_size = eh->key_size + eh->value_size + sizeof(EntryHeader);

        //if we have a gap, move entry further back
        if (ep->offset + entry_size < write_entry) {
            memmove(data + write_entry - entry_size, data + ep->offset, entry_size);
            write_entry -= entry_size;

            ep->offset = write_entry;
        }

        //if we have an entry pointer gap, move the entry pointer to the front
        if (write_ep < read_ep) {
            memmove(data + write_ep, ep, sizeof(EntryPosition));
            write_ep += sizeof(EntryPosition);
        }

        read_ep += sizeof(EntryPosition);

    }

    offset_end = write_ep;
    data_begin = write_entry;

}

double Buffer::get_usage() {
    return 1 - (data_begin - offset_end) / (SIZE * 1.0);
}


