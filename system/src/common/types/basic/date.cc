// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

// Project include
#include "common/types/basic/date.h"
#include "common/types/basic/timestamp.h"

bool Date::operator==(const Timestamp& other) const {
    return get_ts() == other.get_ts();
}

bool Date::operator!=(const Timestamp& other) const {
    return !(*this  == other); 
}

bool Date::operator< (const Timestamp& other) const {
    return get_ts() < other.get_ts();
}

bool Date::operator> (const Timestamp& other) const {
    return other < *this;
}

bool Date::operator<=(const Timestamp& other) const {
    return !(*this > other);
}

bool Date::operator>=(const Timestamp& other) const {
    return !(*this < other);
}
