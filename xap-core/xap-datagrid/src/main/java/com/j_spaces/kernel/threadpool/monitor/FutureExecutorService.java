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

package com.j_spaces.kernel.threadpool.monitor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * {@link ExecutorService} limited to methods using {@link Future}.
 *
 * @author Moran Avigdor
 * @since 7.0.2
 */
public interface FutureExecutorService {
    /**
     * Initiates an orderly shutdown in which previously submitted tasks are executed, but no new
     * tasks will be accepted. Invocation has no additional effect if already shut down.
     *
     * @throws SecurityException if a security manager exists and shutting down this ExecutorService
     *                           may manipulate threads that the caller is not permitted to modify
     *                           because it does not hold {@link java.lang.RuntimePermission}<tt>("modifyThread")</tt>,
     *                           or the security manager's <tt>checkAccess</tt> method denies
     *                           access.
     */
    void shutdown();

    /**
     * Attempts to stop all actively executing tasks, halts the processing of waiting tasks, and
     * returns a list of the tasks that were awaiting execution.
     *
     * <p>There are no guarantees beyond best-effort attempts to stop processing actively executing
     * tasks.  For example, typical implementations will cancel via {@link Thread#interrupt}, so if
     * any tasks mask or fail to respond to interrupts, they may never terminate.
     *
     * @return list of tasks that never commenced execution
     * @throws SecurityException if a security manager exists and shutting down this ExecutorService
     *                           may manipulate threads that the caller is not permitted to modify
     *                           because it does not hold {@link java.lang.RuntimePermission}<tt>("modifyThread")</tt>,
     *                           or the security manager's <tt>checkAccess</tt> method denies
     *                           access.
     */
    List<Runnable> shutdownNow();

    /**
     * Submits a value-returning task for execution and returns a Future representing the pending
     * results of the task.
     *
     * <p> If you would like to immediately block waiting for a task, you can use constructions of
     * the form <tt>result = exec.submit(aCallable).get();</tt>
     *
     * <p> Note: The {@link Executors} class includes a set of methods that can convert some other
     * common closure-like objects, for example, {@link java.security.PrivilegedAction} to {@link
     * Callable} form so they can be submitted.
     *
     * @param task the task to submit
     * @return a Future representing pending completion of the task
     * @throws RejectedExecutionException if task cannot be scheduled for execution
     * @throws NullPointerException       if task null
     */
    <T> Future<T> submit(Callable<T> task);

    /**
     * Submits a Runnable task for execution and returns a Future representing that task that will
     * upon completion return the given result
     *
     * @param task   the task to submit
     * @param result the result to return
     * @return a Future representing pending completion of the task, and whose <tt>get()</tt> method
     * will return the given result upon completion.
     * @throws RejectedExecutionException if task cannot be scheduled for execution
     * @throws NullPointerException       if task null
     */
    <T> Future<T> submit(Runnable task, T result);

    /**
     * Submits a Runnable task for execution and returns a Future representing that task.
     *
     * @param task the task to submit
     * @return a Future representing pending completion of the task, and whose <tt>get()</tt> method
     * will return <tt>null</tt> upon completion.
     * @throws RejectedExecutionException if task cannot be scheduled for execution
     * @throws NullPointerException       if task null
     */
    Future<?> submit(Runnable task);

    /**
     * Executes the given tasks, returning a list of Futures holding their status and results when
     * all complete. {@link Future#isDone} is <tt>true</tt> for each element of the returned list.
     * Note that a <em>completed</em> task could have terminated either normally or by throwing an
     * exception. The results of this method are undefined if the given collection is modified while
     * this operation is in progress.
     *
     * @param tasks the collection of tasks
     * @return A list of Futures representing the tasks, in the same sequential order as produced by
     * the iterator for the given task list, each of which has completed.
     * @throws InterruptedException       if interrupted while waiting, in which case unfinished
     *                                    tasks are cancelled.
     * @throws NullPointerException       if tasks or any of its elements are <tt>null</tt>
     * @throws RejectedExecutionException if any task cannot be scheduled for execution
     */

    <T> List<Future<T>> invokeAll(Collection<Callable<T>> tasks) throws InterruptedException;

    /**
     * Executes the given tasks, returning a list of Futures holding their status and results when
     * all complete or the timeout expires, whichever happens first. {@link Future#isDone} is
     * <tt>true</tt> for each element of the returned list. Upon return, tasks that have not
     * completed are cancelled. Note that a <em>completed</em> task could have terminated either
     * normally or by throwing an exception. The results of this method are undefined if the given
     * collection is modified while this operation is in progress.
     *
     * @param tasks   the collection of tasks
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return A list of Futures representing the tasks, in the same sequential order as produced by
     * the iterator for the given task list. If the operation did not time out, each task will have
     * completed. If it did time out, some of these tasks will not have completed.
     * @throws InterruptedException       if interrupted while waiting, in which case unfinished
     *                                    tasks are cancelled.
     * @throws NullPointerException       if tasks, any of its elements, or unit are <tt>null</tt>
     * @throws RejectedExecutionException if any task cannot be scheduled for execution
     */
    <T> List<Future<T>> invokeAll(Collection<Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException;
}
