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


package org.openspaces.core.transaction.internal;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;

import net.jini.core.transaction.Transaction;

import org.openspaces.core.GigaSpace;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author kimchy
 */
public class InternalAsyncFuture<T> implements AsyncFuture<T> {

    private final AsyncFuture<T> future;

    private final GigaSpace gigaSpace;

    private final Transaction tx;

    public InternalAsyncFuture(AsyncFuture<T> future, GigaSpace gigaSpace) {
        this(future, gigaSpace, null);
    }

    public InternalAsyncFuture(AsyncFuture<T> future, GigaSpace gigaSpace, Transaction tx) {
        this.future = future;
        this.gigaSpace = gigaSpace;
        this.tx = tx;
    }

    public void setListener(AsyncFutureListener<T> listener) {
        if (tx != null) {
            future.setListener(InternalAsyncFutureListener.wrapIfNeeded(listener, gigaSpace));
        } else {
            future.setListener(listener);
        }
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    public boolean isCancelled() {
        return future.isCancelled();
    }

    public boolean isDone() {
        return future.isDone();
    }

    public T get() throws InterruptedException, ExecutionException {
        try {
            return get(-1, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // will not happen...
            throw new ExecutionException(e);
        }
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            if (timeout == -1) {
                return future.get();
            }
            return future.get(timeout, unit);
        } catch (ExecutionException e) {
            Exception translatedException = gigaSpace.getExceptionTranslator().translateNoUncategorized(e);
            if (translatedException != null) {
                e = new ExecutionException(e.getMessage(), translatedException);
            }
            throw e;
        }
    }

}
