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

package com.j_spaces.kernel.threadpool.queue;

import com.j_spaces.kernel.threadpool.policy.ForceQueuePolicy;
import com.j_spaces.kernel.threadpool.policy.TimedBlockingPolicy;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Much like a {@link SynchronousQueue} which acts as a rendezvous channel. It is well suited for
 * handoff designs, in which a tasks is only queued if there is an available thread to pick it up.
 * <p> This queue is correlated with a thread-pool, and allows insertions to the queue only if there
 * is a free thread that can poll this task. Otherwise, the task is rejected and the decision is
 * left up to one of the {@link RejectedExecutionHandler} policies: <ol> <li> {@link
 * ForceQueuePolicy} - forces the queue to accept the rejected task. </li> <li> {@link
 * TimedBlockingPolicy} - waits for a given time for the task to be executed.</li> </ol>
 *
 * @author moran
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class DynamicQueue<E> extends LinkedBlockingQueue<E> {
    private static final long serialVersionUID = 1L;

    /**
     * The executor this Queue belongs to
     */
    private ThreadPoolExecutor executor;

    /**
     * Creates a <tt>DynamicQueue</tt> with a capacity of {@link Integer#MAX_VALUE}.
     */
    public DynamicQueue() {
        super();
    }

    /**
     * Creates a <tt>DynamicQueue</tt> with the given (fixed) capacity.
     *
     * @param capacity the capacity of this queue.
     */
    public DynamicQueue(int capacity) {
        super(capacity);
    }

    /**
     * Sets the executor this queue belongs to.
     */
    public void setThreadPoolExecutor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    /**
     * Inserts the specified element at the tail of this queue if there is at least one available
     * thread to run the current task. If all pool threads are actively busy, it rejects the offer.
     *
     * @param o the element to add.
     * @return <tt>true</tt> if it was possible to add the element to this queue, else
     * <tt>false</tt>
     * @see ThreadPoolExecutor#execute(Runnable)
     */
    @Override
    public boolean offer(E o) {
        int allWorkingThreads = executor.getActiveCount() + super.size();
        return allWorkingThreads < executor.getPoolSize() && super.offer(o);
    }
}
