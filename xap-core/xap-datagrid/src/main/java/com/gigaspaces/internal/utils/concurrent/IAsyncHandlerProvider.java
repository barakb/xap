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

package com.gigaspaces.internal.utils.concurrent;


/**
 * Provides {@link IAsyncHandler} for repetitive execution of runnables
 *
 * @author eitany
 * @since 8.0
 */
public interface IAsyncHandlerProvider {
    public enum CycleResult {
        /**
         * Cycle is idle, wait for idle delay timeout until next execution
         */
        IDLE_CONTINUE,
        /**
         * Cycle is busy, immedetialy continue with a following execution
         */
        CONTINUE,
        /**
         * Terminate the execution life cycle of the runnable
         */
        TERMINATE,
        /**
         * Suspend the async handler life cycle until a following {@link IAsyncHandler#wakeUp()},
         * {@link IAsyncHandler#wakeUpAndWait(long, java.util.concurrent.TimeUnit)}, {@link
         * IAsyncHandler#resume()()} or {@link IAsyncHandler#stop(long,
         * java.util.concurrent.TimeUnit))} operation is called
         *
         * @since 8.0.5
         */
        SUSPEND;
    }

    /**
     * Start executing a runnable in a repetitive manner.
     *
     * @param callable                 the runnable to execute at each iteration
     * @param idleDelayMilis           the delay between each iteration when the runnable returned a
     *                                 {@link CycleResult#IDLE_CONTINUE}
     * @param name                     hint name for the async handler if supported
     * @param waitIdleDelayBeforeStart should the task run for the first time only after
     *                                 idleDelayMillis
     * @return async handler that allows interaction with the repetitive execution of the runnable
     *
     * <note> If this method is executed after the provider is a closed, the result handler will be
     * terminated on execution {@link IAsyncHandler#isTerminated()}.
     */
    IAsyncHandler start(AsyncCallable callable, long idleDelayMilis, String name, boolean waitIdleDelayBeforeStart);

    /**
     * Start executing a runnable in a repetitive manner, the task may block during its process so
     * the provider must be able to support this. in terms of thread pool limitations
     *
     * @param callable                 the runnable to execute at each iteration
     * @param idleDelayMilis           the delay between each iteration when the runnable returned a
     *                                 {@link CycleResult#IDLE_CONTINUE}
     * @param name                     hint name for the async handler if supported
     * @param waitIdleDelayBeforeStart should the task run for the first time only after
     *                                 idleDelayMillis
     * @return async handler that allows interaction with the repetitive execution of the runnable
     *
     * <note> If this method is executed after the provider is a closed, the result handler will be
     * terminated on execution {@link IAsyncHandler#isTerminated()}.
     */
    IAsyncHandler startMayBlock(AsyncCallable callable, long idleDelayMilis, String name, boolean waitIdleDelayBeforeStart);

    /**
     * Close the async handler provider
     */
    void close();

    /**
     * Specifies is this async handler provider is already closed
     */
    boolean isClosed();

    /**
     * Adds a state listener to be notified on this provider state changes events
     *
     * @return false if this provider is already closed
     */
    boolean addStateListener(IAsyncHandlerProviderStateListener listener);

    /**
     * Removes the specified state listener
     */
    void removeStateListener(IAsyncHandlerProviderStateListener listener);
}
