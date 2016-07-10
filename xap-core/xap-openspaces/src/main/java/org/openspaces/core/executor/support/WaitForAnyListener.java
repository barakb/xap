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


package org.openspaces.core.executor.support;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A listener that can be used to set on several execution and then wait for any of the results
 * (using {@link #waitForResult()}. The number of executions needs to be known in advance and set in
 * the constructor.
 *
 * @author kimchy
 */
public class WaitForAnyListener<T> implements AsyncFutureListener<T> {

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition resultArrived = lock.newCondition();

    private final int numberOfResults;

    private int numberOfResultsArrived;

    private T result;

    private final AsyncFutureListener<T> listener;

    /**
     * Constructs a new listener with the number of executions this listener will be set on.
     */
    public WaitForAnyListener(int numberOfResults) {
        this(numberOfResults, null);
    }

    /**
     * Constructs a new listener with the number of executions this listener will be set on with an
     * optional delegate listener.
     */
    public WaitForAnyListener(int numberOfResults, AsyncFutureListener<T> listener) {
        this.numberOfResults = numberOfResults;
        this.listener = listener;
    }

    public void onResult(AsyncResult<T> result) {
        if (listener != null) {
            listener.onResult(result);
        }
        lock.lock();
        try {
            if (result.getException() == null) {
                this.result = result.getResult();
                resultArrived.signalAll();
            }
            if (++numberOfResultsArrived == numberOfResults) {
                resultArrived.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits for any result indefently.
     */
    public T waitForResult() throws InterruptedException {
        try {
            return waitForResult(-1, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // shoudl not happen
            throw new RuntimeException("Should not occur as we are waiting forever");
        }
    }

    /**
     * Waits for any result for the given time period.
     */
    public T waitForResult(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        lock.lock();
        try {
            if (result != null) {
                return result;
            }
            if (timeout == -1) {
                resultArrived.await();
            } else {
                resultArrived.await(timeout, unit);
            }
            if (result != null) {
                return result;
            }
        } finally {
            lock.unlock();
        }
        throw new TimeoutException("Timeout waiting for result for [" + timeout + "]");
    }
}
