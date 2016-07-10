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


package org.openspaces.core;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultsReducer;
import com.gigaspaces.async.FutureFactory;

import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.Task;
import org.openspaces.core.internal.InternalGigaSpace;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * The executor builder allows to combine all the different types of executing tasks into a single
 * execution (of course, the execution itself, of all the tasks, is asynchronous). Similar to map
 * reduce where first all the tasks are defined, and then executed, and later on, reduced.
 *
 * @author kimchy
 * @see org.openspaces.core.GigaSpace#executorBuilder(com.gigaspaces.async.AsyncResultsReducer)
 */
public class ExecutorBuilder<T extends Serializable, R> {

    final private ArrayList<Holder> holders = new ArrayList<Holder>();

    final private InternalGigaSpace gigaSpace;

    final private AsyncResultsReducer<T, R> reducer;

    /**
     * Constructs a new executor builder, with the {@link org.openspaces.core.GigaSpace} they will
     * execute with, and a reducer to reduce all the different tasks results.
     *
     * <p>The reducer can optionally implement {@link com.gigaspaces.async.AsyncResultFilter} that
     * can control if tasks should continue to accumulate or it should break and execute the reduce
     * operation on the results received so far.
     */
    ExecutorBuilder(GigaSpace gigaSpace, AsyncResultsReducer<T, R> reducer) {
        this.gigaSpace = (InternalGigaSpace) gigaSpace;
        this.reducer = reducer;
    }

    /**
     * Adds a task to be executed similarly to {@link org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.Task)}.
     *
     * @param task The task to add
     * @return The executor builder to add more tasks to or {@link #execute()}.
     * @see org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.Task)
     */
    public ExecutorBuilder<T, R> add(Task<T> task) {
        holders.add(new Holder(task, null));
        return this;
    }

    /**
     * Adds a task to be executed similarly to {@link org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.Task)}.
     *
     * @param task The task to add
     * @return The executor builder to add more tasks to or {@link #execute()}.
     * @see org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.Task)
     */
    public ExecutorBuilder<T, R> add(Task<T> task, AsyncFutureListener<T> listener) {
        holders.add(new Holder(task, null));
        return this;
    }

    /**
     * Adds a task to be executed similarly to {@link org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.Task,
     * Object)}.
     *
     * @param task The task to add
     * @return The executor builder to add more tasks to or {@link #execute()}.
     * @see org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.Task, Object)
     */
    public ExecutorBuilder<T, R> add(Task<T> task, Object routing) {
        holders.add(new Holder(task, routing));
        return this;
    }

    /**
     * Adds a task to be executed similarly to {@link org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.DistributedTask,
     * Object[])}
     *
     * @param task The task to add
     * @return The executor builder to add more tasks to or {@link #execute()}.
     * @see org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.DistributedTask,
     * Object[])
     */
    public ExecutorBuilder<T, R> add(DistributedTask<T, R> task, Object... routing) {
        holders.add(new Holder(task, routing));
        return this;
    }

    /**
     * Adds a task to be executed similarly to {@link org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.DistributedTask)}
     *
     * @param task The task to add
     * @return The executor builder to add more tasks to or {@link #execute()}.
     * @see org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.DistributedTask)
     */
    public ExecutorBuilder<T, R> add(DistributedTask<T, R> task) {
        holders.add(new Holder(task, null));
        return this;
    }

    /**
     * Executes all the given tasks (asynchronously) based on their execution mode and returns a
     * future allowing to retrieve the reduced operation of all the tasks.
     *
     * <p>The future actual result will be the reduced result of the execution, or the exception
     * thrown during during the reduce operation. The moderator (assuming the reducer provided
     * implements {@link com.gigaspaces.async.AsyncResultFilter}) can be used as a mechanism to
     * listen for results as they arrive.
     *
     * @return a Future representing pending completion of the task, and whose <code>get()</code>
     * method will return the task value upon completion.
     */
    public AsyncFuture<R> execute() {
        if (holders.size() == 0) {
            throw new IllegalArgumentException("No tasks to execute");
        }
        AsyncFuture[] futures = new AsyncFuture[holders.size()];
        for (int i = 0; i < futures.length; i++) {
            Holder holder = holders.get(i);
            if (holder.task instanceof DistributedTask) {
                if (holder.routing != null) {
                    futures[i] = gigaSpace.execute((DistributedTask) holder.task, (Object[]) holder.routing);
                } else {
                    futures[i] = gigaSpace.execute((DistributedTask) holder.task);
                }
            } else {
                if (holder.routing != null) {
                    futures[i] = gigaSpace.execute(holder.task, holder.routing);
                } else {
                    futures[i] = gigaSpace.execute(holder.task);
                }
            }
        }
        AsyncFuture<R> result;
        if (reducer instanceof AsyncResultFilter) {
            result = FutureFactory.create(futures, reducer, (AsyncResultFilter<T>) reducer);
        } else {
            result = FutureFactory.create(futures, reducer);
        }
        return gigaSpace.wrapFuture(result, gigaSpace.getCurrentTransaction());
    }

    private static class Holder<S extends Serializable> {
        Task<S> task;
        Object routing;

        public Holder(Task<S> task, Object routing) {
            this.task = task;
            this.routing = routing;
        }
    }
}
