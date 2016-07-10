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

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultFilterEvent;
import com.gigaspaces.async.AsyncResultsReducer;
import com.gigaspaces.lrmi.nio.async.FutureContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * a future implementation used to aggregate results from multiple Futures. it uses a call back from
 * the single Futures to determine whether the action has completed.
 *
 * @author asy ronen
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class CompoundFuture<T, R> implements AsyncFuture<R>, AsyncFutureListener<T> {

    private final List<AsyncResult<T>> replies;

    private volatile boolean canceled = false;

    private final int maxResults;

    private final AsyncResultsReducer<T, R> reducer;

    private final AsyncResultFilter<T> moderator;

    final private Lock _lock = new ReentrantLock();
    final private Condition _resultCondition = _lock.newCondition();

    private volatile boolean done = false;

    private volatile AsyncFutureListener<R> listener;

    private int receivedResults = 0;

    public CompoundFuture(AsyncFuture<T>[] futures, AsyncResultsReducer<T, R> reducer) {
        this(futures, reducer, null);
    }

    public CompoundFuture(AsyncFuture<T>[] futures, AsyncResultsReducer<T, R> reducer, AsyncResultFilter<T> moderator) {
        this.reducer = reducer;
        this.moderator = moderator;
        this.replies = new ArrayList<AsyncResult<T>>(futures.length);
        maxResults = futures.length;

        this.listener = FutureContext.getFutureListener();
        FutureContext.clear();

        // must be performed AFTER setting the listener because registration can potentially call
        // the listener if all(enough) results are available.
        for (AsyncFuture<T> future : futures) {
            future.setListener(this);
        }
    }

    public void setListener(AsyncFutureListener<R> listener) {
        this.listener = listener;
        if (isCancelled() || isDone()) {
            R result = null;
            Exception exp = null;
            try {
                result = getResult();
            } catch (ExecutionException e) {
                exp = (Exception) e.getCause();
            }

            listener.onResult(new DefaultAsyncResult<R>(result, exp));
        }
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        canceled = true;
        if (mayInterruptIfRunning && !isDone()) {

            try {
                _lock.lock();

                _resultCondition.signalAll();
            } finally {
                _lock.unlock();
            }
        }
        return true;
    }


    public boolean isCancelled() {
        return canceled;
    }

    public boolean isDone() {
        return done;
    }

    private AsyncResultFilter.Decision invokeAsyncResultFilter(AsyncResult<T> current) {
        if (moderator == null) {
            return AsyncResultFilter.Decision.CONTINUE;
        }

        //Collections.
        AsyncResult<T>[] asyncResults = replies.toArray(new AsyncResult[replies.size()]);
        AsyncResultFilterEvent<T> event = new AsyncResultFilterEvent<T>(current, asyncResults, maxResults);
        AsyncResultFilter.Decision decision = moderator.onResult(event);

        return decision;
    }

    public R get() throws InterruptedException, ExecutionException {

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

    public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

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

    public void onResult(AsyncResult<T> result) {
        synchronized (replies) {
            if (done) {
                return;
            }
            //Not volatile because assumes this method is called under synchronized block
            receivedResults++;

            boolean lastResult = receivedResults == maxResults;

            AsyncResultFilter.Decision decision = invokeAsyncResultFilter(result);
            switch (decision) {
                case CONTINUE:
                    replies.add(result);
                    break;
                case SKIP:
                    break;
                case BREAK:
                    replies.add(result);
                    lastResult = true;
                    break;
                case SKIP_AND_BREAK:
                    lastResult = true;
                    break;
            }
            if (!lastResult)
                return;

            done = true;
        }

        try {
            _lock.lock();
            _resultCondition.signalAll();
        } finally {
            _lock.unlock();
        }

        if (listener != null) {
            R res = null;
            Exception exp = null;
            try {
                res = getResult();
            } catch (ExecutionException e) {
                exp = (Exception) e.getCause();
            }

            listener.onResult(new DefaultAsyncResult<R>(res, exp));
        }
    }

    private R getResult() throws ExecutionException {
        List<AsyncResult<T>> res;
        synchronized (replies) {
            res = new ArrayList<AsyncResult<T>>(replies.size());
            for (AsyncResult<T> replyPair : replies) {
                res.add(replyPair);
            }
        }

        try {
            return reducer.reduce(res);
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    /**
     * Throws CancellationException if the future was canceled.
     */
    private void checkState() {
        if (isCancelled())
            throw new CancellationException("task was cancelled");
    }
}
