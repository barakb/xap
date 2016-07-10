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

import com.gigaspaces.async.AsyncResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A wrapper that implemetns {@link Future} that wras an already arrived {@link
 * com.gigaspaces.async.AsyncResult}.
 *
 * @author kimchy
 */
public class AsyncResultFuture<T> implements Future<T> {

    private final AsyncResult<T> result;

    public AsyncResultFuture(AsyncResult<T> result) {
        this.result = result;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return true;
    }

    public T get() throws InterruptedException, ExecutionException {
        if (result.getException() != null) {
            throw new ExecutionException(result.getException());
        }
        return result.getResult();
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }
}
