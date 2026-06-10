#pragma once
// ----------------------------------------------------------------------------
// CPU affinity (pinning).
// Pinning the strategy thread to an isolated core keeps its working set hot
// in L1/L2 and removes scheduler migrations.
// ----------------------------------------------------------------------------
#include <cstdint>
#include <string>

#if defined(_WIN32)
  #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
  #endif
  #include <windows.h>
#else
  #include <pthread.h>
  #include <sched.h>
#endif

namespace affinity {

inline bool pin_this_thread(int core) {
    if (core < 0) return false;
#if defined(_WIN32)
    const DWORD_PTR mask = DWORD_PTR(1) << core;
    return SetThreadAffinityMask(GetCurrentThread(), mask) != 0;
#else
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(core, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#endif
}

// Raise priority of the current thread (needs privilege if you are using Linux by the way).
inline void boost_priority() {
#if defined(_WIN32)
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
#else
    sched_param sp{}; sp.sched_priority = 80;
    pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp); // may fail w/o CAP_SYS_NICE
#endif
}

} // namespace affinity
