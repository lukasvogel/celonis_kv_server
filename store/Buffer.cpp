//
// Created by Lukas on 09.03.2018.
//

#include "Buffer.h"
#include "EntryHeader.h"

void Buffer::put(string key, string value) {
    // first delete a potentially already existing entry (for update)
    del(key);

    int64_t free_space = (data_begin - offset_end);

    size_t key_size = key.size() + 1; // null termination
    size_t value_size = value.size() + 1;
    size_t entry_size = key_size + value_size + sizeof(EntryHeader);

    if (free_space >= entry_size + sizeof(EntryPosition)) {
        // we have enough room, write the data
        insert(key, value);
    } else {
        // not enough space, first try compacting
        //TODO: maybe do not compact as aggresssive (currently: always compact if no space even if was compacted on the insert before)
        cout << "compacting..." << endl;
        cout << "usage before: " << get_usage() << endl;
        compact();
        cout << "usage after: " << get_usage() << endl;

        // retry insertion
        if (free_space >= entry_size + sizeof(EntryPosition)) {
            // we have enough room, write the data
            insert(key, value);
        } else {
            // if still not enough space, split
            //TODO: split
        }
    }

    cout << "usage: " << get_usage() << endl;
}

string Buffer::get(string key) {

    EntryPosition *ep;
    EntryHeader *eh;
    //TODO: handle not found case
    if (find(key, &ep, &eh)) {
        char *cur_value = data + ep->offset + sizeof(EntryHeader) + eh->key_size;
        return string(cur_value);
    } else {
        return nullptr;
    }
}

void Buffer::del(string key) {
    EntryPosition *ep;
    EntryHeader *eh;

    if (find(key, &ep, &eh)) {
        cout << "deleting entry: " << key << endl;
        ep->status = EntryPosition::Status::DELETED;
    }
}

bool Buffer::find(string key, EntryPosition **position, EntryHeader **header) {
    // We iterate from the back so we do not look at deleted entries first
    size_t offset = offset_end;

    while (offset > 0) {
        auto *ep = reinterpret_cast<EntryPosition *>(data + offset - sizeof(EntryPosition));
        auto *eh = reinterpret_cast<EntryHeader *>(data + ep->offset);

        char *cur_key = data + ep->offset + sizeof(EntryHeader);

        if (ep->status == EntryPosition::Status::ACTIVE && string(cur_key) == key) {
            *position = ep;
            *header = eh;
            return true;
        }
        offset -= sizeof(EntryPosition);
    }
    return false;
}

void Buffer::insert(string key, string value) {
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
    EntryPosition ep(data_begin);
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

        //if we have an entry pointer gap, move the entry pointer to the front
        if (write_ep < read_ep) {
            memmove(data + write_ep, data + read_ep, sizeof(EntryPosition));
        }

        auto *eh = reinterpret_cast<EntryHeader *>(data + ep->offset);
        size_t entry_size = eh->key_size + eh->value_size;

        //if we have a gap, move entry further back
        if (write_entry > ep->offset + entry_size) {
            memmove(data + write_entry - eh->value_size, data + ep->offset + eh->key_size,
                    eh->value_size); //move the value back
            write_ep += eh->value_size;
            memmove(data + write_entry - eh->key_size, data + ep->offset, eh->key_size); //move the key back
            write_ep += eh->key_size;
            ep->offset = write_entry;
        }

        read_ep += sizeof(EntryPosition);

    }

}

double Buffer::get_usage() {
    return 1 - (data_begin - offset_end) / (SIZE * 1.0);
}

