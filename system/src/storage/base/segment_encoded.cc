// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Related include
#include "storage/base/segment_encoded.h"

namespace storage {

template <>
int DeltaDict<Varchar_T>::getVid(String value, bool insert) {
    int vid = -1;
    if (!value_map_.find(value, vid) && insert) {
        String new_val = value.clone();
        vid = value_id_.fetch_add(1);
        value_map_.insert(new_val, vid);

        WriteLock write_lock(mx_dict_);
        while ((int)values_.size() <= vid) values_.push_back(new_val);
        values_[vid] = new_val;
    }

    return vid;
}

template <>
int DeltaDict<Char_T>::getVid(String value, bool insert) {
    int vid = -1;
    if (!value_map_.find(value, vid) && insert) {
        String new_val = value.clone();
        vid = value_id_.fetch_add(1);
        value_map_.insert(new_val, vid);

        WriteLock write_lock(mx_dict_);
        while ((int)values_.size() <= vid) values_.push_back(new_val);
        values_[vid] = new_val;
    }

    return vid;
}

template <>
uint32_t DeltaDict<Varchar_T>::getVarSize() {
    uint32_t size = 0;
    auto itr = value_map_.begin();

    for ( ; itr; ++itr) {
        uint16_t var_size = itr.first().size();
        if (var_size > MAX_EMBEDDED_SIZE)
            size += var_size;
    }

    return size;
}

template <>
void SegmentEncoded<Varchar_T>::writeEmbeddedDict(std::vector<String>& values) {
    if (sgmt_type_ == DYNAMIC) {
        std::string err("wrong segment type for encoding");
        RTI_EXCEPTION(err);
    }

    Varchar_T* dict = reinterpret_cast<Varchar_T*>(this->getDictPtr(0));

    uint32_t var_size = values.size() *sizeof (Varchar_T);
    auto itr = values.begin();
    for (unsigned idx = 0; itr != values.end(); itr++, idx++) {
        uint32_t var_offset = var_size - idx * sizeof(Varchar_T);
        uint16_t size = itr->size();

        if (size <= MAX_EMBEDDED_SIZE) {
            new (&(dict[idx])) Varchar_T(*itr, size);
        } else {
            new (&(dict[idx])) Varchar_T(*itr, size, var_offset);
            var_size += size;
        }
    }
}

}  // namespace storage
