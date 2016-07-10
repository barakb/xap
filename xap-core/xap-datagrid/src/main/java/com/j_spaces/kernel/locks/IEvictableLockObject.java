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
 * @since 6.6 the  interface for a lock object that represents an evictable subject
 */
public interface IEvictableLockObject
        extends ILockObject {
    /**
     * true if this object is an eviction indicator. regular locks will wait till eviction
     * performed, eviction will not take place if the object is locked
     *
     * @return true if it is
     */
    public boolean isEvictionPermissionIndicator();

    /**
     * get the number of users using (i.e locking + waiting + finished & not released yet) of the
     * lock object
     *
     * @return the num of users
     */
    public int getNumOfUsers();

    /**
     * increment the number of users using (i.e locking + waiting + finished & not released yet) of
     * the lock object, if the lock object is not empty (zero users)
     *
     * @return true if # of users incremented, false if empty
     */
    public boolean incrementNumOfUsersIfNotEmpty();

    /**
     * decrement the number of users using (i.e locking + waiting + finished & not released yet) of
     * the lock object
     *
     * @return true if lock object is empty (i.e. zero users reached)
     */
    public boolean decrementNumOfUsersAndIndicateEmpty();

    /**
     * atomicly get & reset the number of users  of the lock object
     *
     * @return the num of users
     */
    public int getNumOfUsersAndEmpty();

    /**
     * get the uid for the object to lock
     *
     * @return the uid
     */
    public String getUID();

}
