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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A listener that can be used to set on several execution and then wait for all the results (using
 * {@link #waitForResult()}. The number of executions needs to be known in advance and set in the
 * constructor.
 *
 * @author kimchy
 */
public class WaitForAllListener<T> implements AsyncFutureListener<T> {

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition resultArrived = lock.newCondition();

    private int numberOfResults;

    private final AsyncFutureListener<T> listener;

    private final Future<T>[] results;

    private int numberOfResultsArrived;

    /**
     * Constructs a new listener with the number of executions this listener will be set on.
     */
    public WaitForAllListener(int numberOfResults) {
        this(numberOfResults, null);
    }

    /**
     * Constructs a new listener with the number of executions this listener will be set on with an
     * optional delegate listener.
     */
    public WaitForAllListener(int numberOfResults, AsyncFutureListener<T> listener) {
        this.listener = listener;
        this.numberOfResults = numberOfResults;
        this.results = new Future[numberOfResults];
    }

    public void onResult(AsyncResult<T> result) {
        if (listener != null) {
            listener.onResult(result);
        }
        lock.lock();
        try {
            results[numberOfResultsArrived++] = new AsyncResultFuture<T>(result);
            if (numberOfResultsArrived == numberOfResults) {
                resultArrived.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits for all the results indefently. The futures retured all are done.
     */
    public Future<T>[] waitForResult() throws InterruptedException {
        return waitForResult(-1, TimeUnit.MILLISECONDS);
    }

    /**
     * Waits for all the results for the given time period.
     */
    public Future<T>[] waitForResult(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            if (numberOfResults == numberOfResultsArrived) {
                return results;
            }
            if (timeout == -1) {
                resultArrived.await();
            } else {
                resultArrived.await(timeout, unit);
            }
            if (numberOfResultsArrived == numberOfResults) {
                return results;
            } else {
                Future<T>[] partialResult = new Future[numberOfResultsArrived];
                System.arraycopy(results, 0, partialResult, 0, numberOfResultsArrived);
                return partialResult;
            }
        } finally {
            lock.unlock();
        }
    }
}