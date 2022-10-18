// Copyright 2020 PIDL(Petabyte-scale In-memory Database Lab) http://kdb.snu.ac.kr

#ifndef UTIL_VTUNE_HELPER_H_
#define UTIL_VTUNE_HELPER_H_

// Other include
#include <ittnotify.h>

// Usage: Wrap target profiled lines by profileStart() and profileStop().
//        And then run 'vtune' as paused
// Run command example:
//        amplxe-cl -collect hotspots -start-paused -- <target_binary> <arguments>
class VTuneHelper {
 public:
    static void profileStart() { __itt_resume(); }
    static void profileStop() { __itt_pause(); }
};

#endif  // UTIL_VTUNE_HELPER_H_
