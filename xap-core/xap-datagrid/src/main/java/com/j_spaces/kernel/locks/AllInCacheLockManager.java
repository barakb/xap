/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.j_spaces.kernel.locks;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 6.6 lock manager implementation for all-in-cache, we always lock the object itself
 */
@com.gigaspaces.api.InternalApi
public class AllInCacheLockManager<T extends ISelfLockingSubject>
        implements IBasicLockManager<T> {

    /*
     * @see com.j_spaces.kernel.locks.IBasicLockManager#getLockObject(java.lang.Object)
     */
    public ILockObject getLockObject(T subject) {
        return getLockObject(subject, false /*isEvictable*/);
    }

    /*
     * @see com.j_spaces.kernel.locks.IBasicLockManager#getLockObject(java.lang.Object, java.lang.Object, boolean)
     */
    public ILockObject getLockObject(T subject, boolean isEvictable) {
        if (isEvictable)
            throw new RuntimeException("AllInCacheLockManager::getLockObject isEvictable=true for all-in-cache");
        return
                subject;
    }

    /**
     * based only on subject's uid, return a lock object in order to lock the represented subject
     * this method is relevant only for evictable objects
     *
     * @return the lock object
     */
    public ILockObject getLockObject(String subjectUid) {
        throw new RuntimeException("AllInCacheLockManager::getLockObject based on uid is irrelevant for all-in-cache");
    }

    /*
     * @see com.j_spaces.kernel.locks.IBasicLockManager#freeLockObject(com.j_spaces.kernel.locks.ILockObject)
     */
    public void freeLockObject(ILockObject lockObject) {
        return;
    }

    /**
     * do we use subject for locking itself ?
     *
     * @param isEvictable - is subject evictable
     * @return true if we use subject
     */
    public boolean isPerLogicalSubjectLockObject(boolean isEvictable) {
        if (isEvictable)
            throw new RuntimeException("AllInCacheLockManager::no evictable object");
        return true;
    }


}
