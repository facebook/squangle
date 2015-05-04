/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
#ifndef THREAD_NAME_HELPER_H
#define THREAD_NAME_HELPER_H

#include <pthread.h>

#ifdef __APPLE__
inline int squangle_pthread_setname(const char *name)
{
    return pthread_setname_np(name);
}
#else
inline int squangle_pthread_setname(const char *name)
{
    return pthread_setname_np(pthread_self(), name);
}
#endif

#endif // THREAD_NAME_HELPER_H
