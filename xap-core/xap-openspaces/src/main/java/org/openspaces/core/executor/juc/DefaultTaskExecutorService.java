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


package org.openspaces.core.executor.juc;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncResultsReducer;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.support.WaitForAllListener;
import org.openspaces.core.executor.support.WaitForAnyListener;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An implementation of an {@link java.util.concurrent.ExecutorService} that uses the executors
 * support implemented in {@link org.openspaces.core.GigaSpace}.
 *
 * @author kimchy
 */
public class DefaultTaskExecutorService implements TaskExecutorService {

    private GigaSpace gigaSpace;

    private volatile boolean shutdown = false;

    public DefaultTaskExecutorService(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    public void execute(Runnable command) {
        if (command instanceof AsyncResultsReducer) {
            gigaSpace.execute(new RunnableDistributedTaskAdapter(command));
        } else {
            gigaSpace.execute(new RunnableTaskAdapter(command));
        }
    }

    public <T> AsyncFuture<T> submit(Callable<T> task) {
        AsyncFuture<T> result;
        if (task instanceof AsyncResultsReducer) {
            result = gigaSpace.execute(new CallableDistributedTaskAdapter(task));
        } else {
            result = gigaSpace.execute(new CallableTaskAdapter(task));
        }
        return result;
    }

    public <T> Future<T> submit(Callable<T> task, Object routing) {
        return gigaSpace.execute(new CallableTaskAdapter(task), routing);
    }

    public AsyncFuture<?> submit(Runnable task) {
        AsyncFuture<?> result;
        if (task instanceof AsyncResultsReducer) {
            result = gigaSpace.execute(new RunnableDistributedTaskAdapter(task));
        } else {
            result = gigaSpace.execute(new RunnableTaskAdapter(task));
        }
        return result;
    }

    public <T> AsyncFuture<T> submit(Runnable task, T result) {
        AsyncFuture<T> future;
        if (task instanceof AsyncResultsReducer) {
            future = gigaSpace.execute(new RunnableDistributedTaskAdapter(task, (Serializable) result));
        } else {
            future = gigaSpace.execute(new RunnableTaskAdapter(task, (Serializable) result));
        }
        return future;
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return invokeAll(tasks, -1, TimeUnit.MILLISECONDS);
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        WaitForAllListener<T> listener = new WaitForAllListener<T>(tasks.size());
        ArrayList<AsyncFuture<T>> results = new ArrayList<AsyncFuture<T>>(tasks.size());
        for (Callable<T> task : tasks) {
            AsyncFuture<T> result;
            if (task instanceof AsyncResultsReducer) {
                result = gigaSpace.execute(new CallableDistributedTaskAdapter(task));
            } else {
                result = gigaSpace.execute(new CallableTaskAdapter(task));
            }
            result.setListener(listener);
            results.add(result);
        }
        Future<T>[] result = listener.waitForResult(timeout, unit);
        for (AsyncFuture<T> future : results) {
            if (!future.isDone()) {
                future.cancel(false);
            }
        }
        return Arrays.asList(result);
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        try {
            return invokeAny(tasks, -1, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // should not get this
            throw new ExecutionException("Timeout waiting for result", e);
        }
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        WaitForAnyListener<T> listener = new WaitForAnyListener<T>(tasks.size());
        ArrayList<AsyncFuture<T>> results = new ArrayList<AsyncFuture<T>>(tasks.size());
        for (Callable<T> task : tasks) {
            AsyncFuture<T> result;
            if (task instanceof AsyncResultsReducer) {
                result = gigaSpace.execute(new CallableDistributedTaskAdapter(task));
            } else {
                result = gigaSpace.execute(new CallableTaskAdapter(task));
            }
            result.setListener(listener);
            results.add(result);
        }
        T result = listener.waitForResult(timeout, unit);
        for (AsyncFuture<T> future : results) {
            if (!future.isDone()) {
                future.cancel(false);
            }
        }
        return result;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    public List<Runnable> shutdownNow() {
        this.shutdown = true;
        return new ArrayList<Runnable>();
    }

    public boolean isShutdown() {
        return this.shutdown;
    }

    public boolean isTerminated() {
        return this.shutdown;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        // TODO can be implemented in a nicer way to comply with the interface on expecnse of overhead
        // of wrapping every returend async future
        return true;
    }

}
