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

package com.j_spaces.kernel.threadpool;

import com.j_spaces.kernel.threadpool.monitor.FutureExecutorService;
import com.j_spaces.kernel.threadpool.monitor.TimeoutObserverExecutorService;
import com.j_spaces.kernel.threadpool.policy.ForceQueuePolicy;
import com.j_spaces.kernel.threadpool.policy.TimedBlockingPolicy;
import com.j_spaces.kernel.threadpool.queue.DynamicQueue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Factory and utility methods for handling {@link DynamicThreadPoolExecutor}.
 *
 * @author moran
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class DynamicExecutors {
    /**
     * Creates a thread pool that creates new threads as needed, but will reuse previously
     * constructed threads when they are available. Calls to <tt>execute</tt> will reuse previously
     * constructed threads if available. If no existing thread is available, a new thread will be
     * created and added to the pool. No more than <tt>max</tt> threads will be created. Threads
     * that have not been used for a <tt>keepAlive</tt> timeout are terminated and removed from the
     * cache. Thus, a pool that remains idle for long enough will not consume any resources other
     * than the <tt>min</tt> specified.
     *
     * @param min           the number of threads to keep in the pool, even if they are idle.
     * @param max           the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the min, this is the maximum
     *                      time that excess idle threads will wait for new tasks before terminating
     *                      (in milliseconds).
     * @return the newly created thread pool
     */
    public static ExecutorService newScalingThreadPool(int min, int max,
                                                       long keepAliveTime) {
        DynamicQueue<Runnable> queue = new DynamicQueue<Runnable>();
        DynamicThreadPoolExecutor executor = new DynamicThreadPoolExecutor(min,
                max,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                queue);
        executor.setRejectedExecutionHandler(new ForceQueuePolicy());
        queue.setThreadPoolExecutor(executor);
        return executor;
    }

    /**
     * Creates a thread pool, same as in {@link #newScalingThreadPool(int, int, long)}, using the
     * provided ThreadFactory to create new threads when needed.
     *
     * @param min           the number of threads to keep in the pool, even if they are idle.
     * @param max           the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the min, this is the maximum
     *                      time that excess idle threads will wait for new tasks before terminating
     *                      (in milliseconds).
     * @param threadFactory the factory to use when creating new threads.
     * @return the newly created thread pool
     */
    public static ExecutorService newScalingThreadPool(int min, int max,
                                                       long keepAliveTime, ThreadFactory threadFactory) {
        ExecutorService executorService = newScalingThreadPool(min,
                max,
                keepAliveTime);
        ((ThreadPoolExecutor) executorService).setThreadFactory(threadFactory);
        return executorService;
    }

    /**
     */
    public static ExecutorService newBlockingThreadPool(int min, int max,
                                                        long keepAliveTime, int capacity, long waitTime) {
        return newBlockingThreadPool(min, max, keepAliveTime, capacity, waitTime, false);
    }

    /**
     * Creates a thread pool similar to that constructed by {@link #newScalingThreadPool(int, int,
     * long)}, but blocks the call to <tt>execute</tt> if the queue has reached it's capacity, and
     * all <tt>max</tt> threads are busy handling requests. <p> If the wait time of this queue has
     * elapsed, a {@link RejectedExecutionException} will be thrown.
     *
     * @param min           the number of threads to keep in the pool, even if they are idle.
     * @param max           the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the min, this is the maximum
     *                      time that excess idle threads will wait for new tasks before terminating
     *                      (in milliseconds).
     * @param capacity      the fixed capacity of the underlying queue (resembles backlog).
     * @param waitTime      the wait time (in milliseconds) for space to become available in the
     *                      queue.
     * @param preStart      whether to pre start all core threads in the pool.
     * @return the newly created thread pool
     */
    public static ExecutorService newBlockingThreadPool(int min, int max,
                                                        long keepAliveTime, int capacity, long waitTime, boolean preStart) {
        DynamicQueue<Runnable> queue = new DynamicQueue<Runnable>(capacity);
        ThreadPoolExecutor executor = new DynamicThreadPoolExecutor(min,
                max,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                queue);
        executor.setRejectedExecutionHandler(new TimedBlockingPolicy(waitTime));
        queue.setThreadPoolExecutor(executor);
        if (preStart)
            executor.prestartAllCoreThreads();

        return executor;
    }

    /**
     */
    public static ExecutorService newBlockingThreadPool(int min, int max,
                                                        long keepAliveTime, int capacity, long waitTime, int priority,
                                                        String poolName, boolean preStart) {
        DynamicQueue<Runnable> queue = new DynamicQueue<Runnable>(capacity);
        ThreadFactory threadFactory = priorityThreadFactory(priority, poolName);
        ThreadPoolExecutor executor = new DynamicThreadPoolExecutor(min,
                max,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                queue);

        executor.setThreadFactory(threadFactory);
        executor.setRejectedExecutionHandler(new TimedBlockingPolicy(waitTime));
        queue.setThreadPoolExecutor(executor);
        if (preStart)
            executor.prestartAllCoreThreads();

        return executor;
    }

    /**
     * Creates a thread pool, same as in {@link #newBlockingThreadPool(int, int, long, int, long)},
     * using the provided ThreadFactory to create new threads when needed.
     *
     * @param min           the number of threads to keep in the pool, even if they are idle.
     * @param max           the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the min, this is the maximum
     *                      time that excess idle threads will wait for new tasks before terminating
     *                      (in milliseconds).
     * @param capacity      the fixed capacity of the underlying queue (resembles backlog).
     * @param waitTime      the wait time (in milliseconds) for space to become available in the
     *                      queue.
     * @param threadFactory the factory to use when creating new threads.
     * @return the newly created thread pool
     */
    public static ExecutorService newBlockingThreadPool(int min, int max,
                                                        long keepAliveTime, int capacity, long waitTime,
                                                        ThreadFactory threadFactory) {
        ExecutorService executorService = newBlockingThreadPool(min,
                max,
                keepAliveTime,
                capacity,
                waitTime);
        ((ThreadPoolExecutor) executorService).setThreadFactory(threadFactory);
        return executorService;
    }


    /**
     * Creates a thread pool, same as in {@link #newScalingThreadPool(int, int, long,
     * ThreadFactory)}, using the provided ThreadFactory to create new threads when needed. The
     * thread pool is backed by a timeout observer which waits for tasks to complete up to the
     * specified <tt>waitTime</tt>. If a future task has not completed by this time, a {@link
     * Thread#interrupt()} is invoked.
     *
     * <p> There are no guarantees beyond best-effort attempts to stop processing actively executing
     * tasks. For example, typical implementations will cancel via {@link Thread#interrupt}, so if
     * any tasks mask or fail to respond to interrupts, they may never terminate.
     *
     * @param min           the number of threads to keep in the pool, even if they are idle.
     * @param max           the maximum number of threads to allow in the pool.
     * @param keepAliveTime when the number of threads is greater than the min, this is the maximum
     *                      time that excess idle threads will wait for new tasks before terminating
     *                      (in milliseconds).
     * @param waitTime      the maximum time to wait before interrupting a task.
     * @param threadFactory the factory to use when creating new threads.
     * @return the newly created thread pool
     */
    public static FutureExecutorService newEventThreadPool(int min, int max, long keepAliveTime, long waitTime,
                                                           ThreadFactory threadFactory) {
        ExecutorService scalingThreadPool = newScalingThreadPool(min, max, keepAliveTime, threadFactory);
        FutureExecutorService futureExecutorService = new TimeoutObserverExecutorService(scalingThreadPool, waitTime);
        return futureExecutorService;
    }

    /**
     * A priority based thread factory, for all Thread priority constants: <tt>Thread.MIN_PRIORITY,
     * Thread.NORM_PRIORITY, Thread.MAX_PRIORITY</tt>; <p> This factory is used instead of
     * Executers.DefaultThreadFactory to allow manipulation of priority and thread owner name.
     *
     * @param namePrefix a name prefix for this thread
     * @return a thread factory based on given priority.
     */
    public static ThreadFactory daemonThreadFactory(String namePrefix) {
        return new DynamicThreadFactory(namePrefix, Thread.NORM_PRIORITY);
    }

    /**
     * A priority based thread factory, for all Thread priority constants: <tt>Thread.MIN_PRIORITY,
     * Thread.NORM_PRIORITY, Thread.MAX_PRIORITY</tt>; <p> This factory is used instead of
     * Executers.DefaultThreadFactory to allow manipulation of priority and thread owner name.
     *
     * @param priority   The priority to be assigned to each thread; can be either
     *                   <tt>Thread.MIN_PRIORITY, Thread.NORM_PRIORITY</tt> or Thread.MAX_PRIORITY.
     * @param namePrefix a name prefix for this thread
     * @return a thread factory based on given priority.
     */
    public static ThreadFactory priorityThreadFactory(int priority, String namePrefix) {
        return new DynamicThreadFactory(namePrefix, priority);
    }

    /**
     * Cannot instantiate.
     */
    private DynamicExecutors() {
    }
}
