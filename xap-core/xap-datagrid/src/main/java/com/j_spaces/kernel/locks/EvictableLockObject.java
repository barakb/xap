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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 6.5 a lock object stored as a representative of the original subject which is evictable
 */
@com.gigaspaces.api.InternalApi
public class EvictableLockObject extends AtomicInteger implements IEvictableLockObject {
    private static final long serialVersionUID = -8146971271147493645L;

    private String _uid;

    //true indicates this object is being evicted
    private boolean _evictionIndicator;

    public EvictableLockObject(String uid) {
        this(uid, false);
    }

    public EvictableLockObject(String uid, boolean evictionIndicator) {
        super(1);
        _uid = uid;
        _evictionIndicator = evictionIndicator;

    }

    /**
     * get the uid for the object to lock
     *
     * @return the uid
     */
    public String getUID() {
        return _uid;
    }

    /**
     * returns true if the lock object is the subject itself (i.e. entry or template) or a
     * representing object
     *
     * @return true if is the subject itself
     */
    public boolean isLockSubject() {
        return false;
    }

    /**
     * true if this object is an eviction indicator. regular locks will wait till eviction
     * performed, eviction will not take place if the object is locked
     *
     * @return true if it is
     */
    public boolean isEvictionPermissionIndicator() {
        return _evictionIndicator;
    }

    /**
     * get the number of users using (i.e locking + waiting + finished & not released yet) of the
     * lock object
     *
     * @return the num of users
     */
    public int getNumOfUsers() {
        return get();
    }

    /**
     * increment the number of users using (i.e locking + waiting + finished & not released yet) of
     * the lock object, if the lock object is not empty (zero users)
     *
     * @return true if # of users incremented, false if empty
     */
    public boolean incrementNumOfUsersIfNotEmpty() {
        while (true) {
            int num = get();
            if (num == 0)
                return false;
            if (compareAndSet(num, num + 1))
                return true;
        }
    }

    /**
     * decrement the number of users using (i.e locking + waiting + finished & not released yet) of
     * the lock object
     *
     * @return true if lock object is empty (i.e. zero users reached)
     */
    public boolean decrementNumOfUsersAndIndicateEmpty() {
        int val = decrementAndGet();
        return (val == 0);
    }


    /**
     * atomicly get & reset the number of users  of the lock object
     *
     * @return the num of users
     */
    public int getNumOfUsersAndEmpty() {
        return getAndSet(0);
    }

    /**
     * if the lock object is an evictable lock object, return the interface (preferable to casing)
     *
     * @return IEvictableLockObject if implemented
     */
    public IEvictableLockObject getEvictableLockObject() {
        return this;
    }

}
