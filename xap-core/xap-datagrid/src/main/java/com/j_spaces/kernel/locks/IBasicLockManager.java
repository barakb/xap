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
 * @since 6.5 contains the basic methods for acuring a lock object- i.e. an object that will be
 * locked in order to lock the entity (entry or template) represented by it
 */
public interface IBasicLockManager<T extends ILockedSubject> {

    /**
     * based on subject, return a lock object in order to lock the represented subject
     *
     * @return the lock object
     */
    public ILockObject getLockObject(T subject);

    /**
     * based on subject, return a lock object in order to lock the subject
     *
     * @param isEvictable = true if the subject is part of evictable cache (like lru)
     * @return the lock object
     */
    public ILockObject getLockObject(T subject, boolean isEvictable);

    /**
     * based only on subject's uid, return a lock object in order to lock the represented subject
     * this method is relevant only for evictable objects
     *
     * @return the lock object
     */
    public ILockObject getLockObject(String subjectUid);

    /**
     * free the lock object- no more needed by this thread
     *
     * @param lockObject the lock object
     */
    public void freeLockObject(ILockObject lockObject);


    /**
     * do we use per-logical subject a different object for locking ?
     *
     * @param isEvictable - is subject evictable
     * @return true if we use subject
     */
    public boolean isPerLogicalSubjectLockObject(boolean isEvictable);

}
