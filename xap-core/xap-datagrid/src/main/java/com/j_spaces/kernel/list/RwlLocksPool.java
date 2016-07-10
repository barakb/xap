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


package com.j_spaces.kernel.list;

import com.j_spaces.kernel.IReusableResourcePool;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Maintains a pool of ReadWriteLock lockers, used by SLs in order to avoid creating an expensive
 * lock per SL (i.e.- index value) used by classes like RwlSegmentedStoredList or RwlStoredList in
 * order to reduce memory footprint.
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class RwlLocksPool
        implements IReusableResourcePool<ReadWriteLock> {
    // lock index
    private static AtomicInteger _lockIndex = new AtomicInteger(0);

    private ReadWriteLock[] _locks;

    public RwlLocksPool(int numLocks) {
        _locks = new ReentrantReadWriteLock[numLocks];
        for (int i = 0; i < numLocks; i++)
            _locks[i] = new ReentrantReadWriteLock();
    }


    public ReadWriteLock getResource(int index) {
        return _locks[index];

    }


    public ReadWriteLock getResource() {
        return _locks[_lockIndex.getAndIncrement() % size()];

    }

    /**
     * get the number of resources in the pool
     *
     * @return pool-size
     */
    public int size() {
        return _locks.length;

    }

}
