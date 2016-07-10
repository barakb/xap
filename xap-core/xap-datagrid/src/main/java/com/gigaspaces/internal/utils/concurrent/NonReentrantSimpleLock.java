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

/**
 * A non reentrant implementation of {@link ISimpleLock} The implementation intent is to save
 * memory, therefore the synchronization is done on the lock object it self, no user should
 * synchronize on the lock object it self as it may create deadlocks.
 *
 * @author eitany
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class NonReentrantSimpleLock
        implements ISimpleLock {

    private Thread _owningThread;
    private int _waiters;

    @Override
    public synchronized boolean tryLock() {
        if (_owningThread != null)
            return false;

        _owningThread = Thread.currentThread();
        return true;
    }

    @Override
    public synchronized void lock() {
        _waiters++;
        boolean wasInterrupted = false;
        while (_owningThread != null) {
            try {
                wait();
            } catch (InterruptedException e) {
                wasInterrupted = true;
            }
        }
        if (wasInterrupted)
            Thread.currentThread().interrupt();
        _owningThread = Thread.currentThread();
        _waiters--;
    }

    @Override
    public synchronized void unlock() {
        Thread currentThread = Thread.currentThread();
        if (_owningThread != currentThread)
            throw new IllegalMonitorStateException(currentThread + " does not hold the lock");
        _owningThread = null;
        if (_waiters > 0)
            notify();
    }

}
