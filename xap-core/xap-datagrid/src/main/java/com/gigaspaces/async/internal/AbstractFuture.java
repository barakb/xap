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

package com.gigaspaces.async.internal;

import com.gigaspaces.lrmi.nio.async.IFuture;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractFuture<T> implements IFuture<T> {
    final protected Lock _lock = new ReentrantLock();
    final protected Condition _resultCondition = _lock.newCondition();

    public T get() throws InterruptedException, ExecutionException {
        try {
            _lock.lock();

            checkState(); // check for canceled

            if (isDone())
                return getResult();

            _resultCondition.await();

            checkState(); // check for canceled 

            return getResult();
        } finally {
            _lock.unlock();
        }
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            _lock.lock();

            checkState(); // check for canceled

            if (isDone())
                return getResult();

            if (!_resultCondition.await(timeout, unit))
                throw new TimeoutException("Timeout waiting for result for [" + timeout + "]");

            checkState(); // check for canceled 

            return getResult();
        } finally {
            _lock.unlock();
        }

    }


    protected abstract T getResult() throws ExecutionException;


    /**
     * Throws CancellationException if the future was canceled.
     */
    private void checkState() {
        if (isCancelled())
            throw new CancellationException("task was cancelled");
    }

}
