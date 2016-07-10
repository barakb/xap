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


package org.openspaces.core.executor;

import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultsReducer;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.juc.CallableDistributedTaskAdapter;
import org.openspaces.core.executor.juc.CallableTaskAdapter;
import org.openspaces.core.executor.juc.DefaultTaskExecutorService;
import org.openspaces.core.executor.juc.RunnableDistributedTaskAdapter;
import org.openspaces.core.executor.juc.RunnableTaskAdapter;
import org.openspaces.core.executor.juc.TaskExecutorService;
import org.openspaces.core.executor.support.PrivilegedDistributedTask;
import org.openspaces.core.executor.support.PrivilegedTask;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * @author kimchy
 */
public class TaskExecutors {

    /**
     * Constructs a new {@link org.openspaces.core.executor.juc.DefaultTaskExecutorService} which is
     * an executor service that can execute tasks on top of the Space.
     */
    public static TaskExecutorService newExecutorService(GigaSpace gigaSpace) {
        return new DefaultTaskExecutorService(gigaSpace);
    }

    /**
     * Constructs a new callable task adapter with the callable to <code>call</code>.
     */
    public static <T extends Serializable> Task<T> task(Callable<T> callable) {
        return new CallableTaskAdapter<T>(callable);
    }

    /**
     * Constructs a new callable distributed task adapter with a separate reducer. The reducer or
     * the callable can optionally implement {@link com.gigaspaces.async.AsyncResultFilter}.
     */
    public static <T extends Serializable, R> DistributedTask<T, R> task(Callable<T> callable, AsyncResultsReducer<T, R> reducer) {
        return new CallableDistributedTaskAdapter<T, R>(callable, reducer);
    }

    /**
     * Constructs a new callable distributed task adapter with a separate reducer and filter.
     */
    public static <T extends Serializable, R> DistributedTask<T, R> task(Callable<T> callable, AsyncResultsReducer<T, R> reducer, AsyncResultFilter<T> filter) {
        return new CallableDistributedTaskAdapter<T, R>(callable, reducer, filter);
    }

    /**
     * Constructs a new runnable task adapter with the runnable to <code>run</code>.
     */
    public static <T extends Serializable> Task<T> task(Runnable runnable) {
        return new RunnableTaskAdapter<T>(runnable);
    }

    /**
     * Constructs a new runnable distributed task adapter with a separate reducer. The reducer or
     * the runnable can optionally implement {@link com.gigaspaces.async.AsyncResultFilter}.
     */
    public static <T extends Serializable, R> DistributedTask<T, R> task(Runnable runnable, AsyncResultsReducer<T, R> reducer) {
        return new RunnableDistributedTaskAdapter<T, R>(runnable, reducer);
    }

    /**
     * Constructs a new runnable distributed task adapter with a separate reducer and filter.
     */
    public static <T extends Serializable, R> DistributedTask<T, R> task(Runnable runnable, AsyncResultsReducer<T, R> reducer, AsyncResultFilter<T> filter) {
        return new RunnableDistributedTaskAdapter<T, R>(runnable, reducer, filter);
    }

    /**
     * Constructs a new runnable distributed task adapter with a separate reducer. The reducer or
     * the runnable can optionally implement {@link com.gigaspaces.async.AsyncResultFilter}.
     */
    public static <T extends Serializable, R> DistributedTask<T, R> task(Runnable runnable, T result, AsyncResultsReducer<T, R> reducer) {
        return new RunnableDistributedTaskAdapter<T, R>(runnable, result, reducer);
    }

    /**
     * Constructs a new runnable distributed task adapter with a separate reducer and filter.
     */
    public static <T extends Serializable, R> DistributedTask<T, R> task(Runnable runnable, T result, AsyncResultsReducer<T, R> reducer, AsyncResultFilter<T> filter) {
        return new RunnableDistributedTaskAdapter<T, R>(runnable, result, reducer, filter);
    }

    /**
     * Constructs a new runnable task adapter with the runnable to <code>run</code>. Will return the
     * result after the call to <code>run</code>.
     */
    public static <T extends Serializable> Task<T> task(Runnable runnable, T result) {
        return new RunnableTaskAdapter<T>(runnable, result);
    }

    /**
     * Constructs a new privileged task wrapping the actual task to execute.
     */
    public static <T extends Serializable> Task<T> privilegedTask(Task<T> task) {
        return new PrivilegedTask<T>(task);
    }

    /**
     * Constructs a new privileged task wrapping the actual task to execute.
     */
    public static <T extends Serializable, R> DistributedTask<T, R> privilegedTask(DistributedTask<T, R> task) {
        return new PrivilegedDistributedTask<T, R>(task);
    }

    /**
     * Constructs a new privileged task wrapping the actual task and filter to execute.
     */
    public static <T extends Serializable, R> DistributedTask<T, R> privilegedTask(DistributedTask<T, R> task, AsyncResultFilter<T> filter) {
        return new PrivilegedDistributedTask<T, R>(task, filter);
    }
}
