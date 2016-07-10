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

package com.gigaspaces.internal.utils.concurrent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A segmented implementation of ReentrantReadWriteLock, providing better concurrency.
 *
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SegmentedReentrantReadWriteLock {
    private final int _numOfSegments;
    private final ReadWriteLock[] _locks;
    private final Lock[] _readLocks;
    private final Lock[] _writeLocks;

    /**
     * Creates a segmented reentrant read write lock using the specified arguments.
     *
     * @param numOfSegments Number of segments to allocate.
     * @param fair          true if this lock should use a fair ordering policy.
     */
    public SegmentedReentrantReadWriteLock(int numOfSegments, boolean fair) {
        if (numOfSegments < 1)
            throw new IllegalArgumentException("Illegal argument 'numOfSegments' - cannot be less than 1.");

        this._numOfSegments = numOfSegments;
        this._locks = new ReadWriteLock[numOfSegments];
        this._readLocks = new Lock[numOfSegments];
        this._writeLocks = new Lock[numOfSegments];
        for (int i = 0; i < numOfSegments; i++) {
            this._locks[i] = new ReentrantReadWriteLock(fair);
            this._readLocks[i] = _locks[i].readLock();
            this._writeLocks[i] = _locks[i].writeLock();
        }
    }

    /**
     * Acquires the write lock. If the lock is not available then the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until the lock has been acquired.
     **/
    public void acquireWriteLock() {
        for (int i = 0; i < _numOfSegments; i++)
            _writeLocks[i].lock();
    }

    /**
     * Releases the write lock.
     **/
    public void releaseWriteLock() {
        for (int i = 0; i < _numOfSegments; i++)
            _writeLocks[i].unlock();
    }

    /**
     * Acquires a read lock on the specified segment. If the lock is not available then the current
     * thread becomes disabled for thread scheduling purposes and lies dormant until the lock has
     * been acquired.
     */
    public void acquireReadLock(int segmentId) {
        _readLocks[segmentId].lock();
    }

    /**
     * Releases a read lock on the specified segment.
     */
    public void releaseReadLock(int segmentId) {
        _readLocks[segmentId].unlock();
    }

    /**
     * Generates a segment ID from the current thread ID.
     *
     * @return a valid segment id.
     */
    public int getSegmentIdByCurrentThreadId() {
        return (int) (Thread.currentThread().getId() % _numOfSegments);
    }

    /**
     * Acquires a read lock on the current thread's segment.
     */
    public void acquireThreadReadLock() {
        _readLocks[getSegmentIdByCurrentThreadId()].lock();
    }

    /**
     * Releases a read lock on the current thread's segment.
     */
    public void releaseThreadReadLock() {
        _readLocks[getSegmentIdByCurrentThreadId()].unlock();
    }
}
