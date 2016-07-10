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

package com.gigaspaces.async;

import com.gigaspaces.async.internal.AbstractFuture;
import com.gigaspaces.async.internal.DefaultAsyncResult;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * Created by Barak Bar Orion 19/08/14.
 */
@com.gigaspaces.api.InternalApi
public class SettableFuture<T> extends AbstractFuture<T> {
    private T result;
    private boolean hasValue;
    private Exception exception;
    private List<AsyncFutureListener<T>> listeners;
    private boolean canceled;
    private Executor executor;


    @SuppressWarnings("UnusedDeclaration")
    public SettableFuture(Executor executor) {
        this.executor = executor;
        listeners = new LinkedList<AsyncFutureListener<T>>();
    }

    @SuppressWarnings("UnusedDeclaration")
    public SettableFuture() {
        this(Executors.newDirectExecutor());
    }

    @Override
    protected T getResult() throws ExecutionException {
        _lock.lock();
        try {
            if (hasValue) {
                return result;
            } else {
                throw new ExecutionException(exception);
            }
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public void setResult(Object result) {
        _lock.lock();
//        if(hasValue || (exception != null)){
//            throw new IllegalStateException("Calling setResult on already resolved future is prohibited, future: " + this + ", value: " + result);
//        }
        List<AsyncFutureListener<T>> listeners = new ArrayList<AsyncFutureListener<T>>(this.listeners);
        try {
            if (result instanceof Throwable) {
                exception = (Exception) result;
            } else {
                //noinspection unchecked
                this.result = (T) result;
                hasValue = true;
            }
            _resultCondition.signalAll();
        } finally {
            _lock.unlock();
        }

        if (!listeners.isEmpty()) {
            DefaultAsyncResult<T> asyncRes = new DefaultAsyncResult<T>(this.result, this.exception);
            for (AsyncFutureListener<T> listener : listeners) {
                fireEvent(asyncRes, listener);
            }
        }
    }

    private void fireEvent(final DefaultAsyncResult<T> asyncRes, final AsyncFutureListener<T> listener) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                listener.onResult(asyncRes);
            }
        });
    }

    @Override
    public void setListener(AsyncFutureListener<T> listener) {
        _lock.lock();
        try {
            if (isDone()) {
                fireEvent(new DefaultAsyncResult<T>(this.result, this.exception), listener);
            }
            listeners.add(listener);
        } finally {
            _lock.unlock();
        }
    }


    @Override
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

    @Override
    public boolean isCancelled() {
        return canceled;
    }

    @Override
    public boolean isDone() {
        _lock.lock();
        try {
            return hasValue || (exception != null);
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "SettableFuture{" + "result=" + result + ", hasValue=" + hasValue + ", exception=" + exception + ", canceled=" + canceled + '}';
    }
}
