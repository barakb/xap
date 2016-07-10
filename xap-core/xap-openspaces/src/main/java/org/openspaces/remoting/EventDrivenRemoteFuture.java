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


package org.openspaces.remoting;

import org.openspaces.core.GigaSpace;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A Space remoting future implementation.
 *
 * @author kimchy
 */
public class EventDrivenRemoteFuture<T> implements Future<T> {

    private final GigaSpace gigaSpace;

    private SpaceRemotingEntry remotingEntry;

    private volatile Boolean cancelled;

    private volatile SpaceRemotingResult remoteResult;

    private SpaceRemotingEntry template;

    public EventDrivenRemoteFuture(GigaSpace gigaSpace, SpaceRemotingEntry remotingEntry) {
        this.gigaSpace = gigaSpace;
        this.remotingEntry = remotingEntry;
        try {
            this.template = ((SpaceRemotingEntry) remotingEntry.clone()).buildResultTemplate();
        } catch (CloneNotSupportedException e) {
            // won't happen
        }
    }

    /**
     * Attempts to cancel execution of this task.  This attempt will fail if the task has already
     * completed, already been cancelled, or could not be cancelled for some other reason. If
     * successful, and this task has not started when <code>cancel</code> is called, this task
     * should never run.
     *
     * @param mayInterruptIfRunning Has no affect when using Space Remoting
     * @return <code>false</code> if the task could not be cancelled, typically because it has
     * already completed normally; <code>true</code> otherwise
     */
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (cancelled != null) {
            return cancelled;
        }
        Object retVal = gigaSpace.take(remotingEntry, 0);
        cancelled = retVal != null;
        return cancelled;
    }

    /**
     * Returns <code>true</code> if this task was cancelled before it completed normally.
     *
     * @return <code>true</code> if task was cancelled before it completed
     */
    public boolean isCancelled() {
        return cancelled != null && cancelled;
    }

    /**
     * Returns <code>true</code> if this task completed.
     *
     * <p>Completion may be due to normal termination, an exception, or cancellation -- in all of
     * these cases, this method will return <code>true</code>.
     *
     * @return <code>true</code> if this task completed.
     */
    public boolean isDone() {
        if (cancelled != null) {
            return true;
        }
        if (remoteResult != null) {
            return true;
        }
        remoteResult = gigaSpace.take(template, 0);
        return remoteResult != null;
    }

    /**
     * Waits if necessary for the computation to complete, and then retrieves its result.
     *
     * @return the computed result
     * @throws CancellationException if the computation was cancelled
     * @throws ExecutionException    if the computation threw an exception
     * @throws InterruptedException  if the current thread was interrupted while waiting
     */
    public T get() throws InterruptedException, ExecutionException {
        if (cancelled != null) {
            throw new CancellationException();
        }
        T retVal = handleResult();
        if (retVal != null) {
            return retVal;
        }
        remoteResult = gigaSpace.take(template, Integer.MAX_VALUE);
        return handleResult();
    }

    /**
     * Waits if necessary for at most the given time for the computation to complete, and then
     * retrieves its result, if available.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return the computed result
     * @throws CancellationException if the computation was cancelled
     * @throws ExecutionException    if the computation threw an exception
     * @throws InterruptedException  if the current thread was interrupted while waiting
     * @throws TimeoutException      if the wait timed out
     */
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (cancelled != null) {
            throw new CancellationException();
        }
        T retVal = handleResult();
        if (retVal != null) {
            return retVal;
        }
        remoteResult = gigaSpace.take(template, unit.toMillis(timeout));
        if (remoteResult == null) {
            throw new TimeoutException("Timeout waiting for remote invocation [" + remotingEntry + "] for [" + unit.toMillis(timeout) + "] milliseconds");
        }
        return handleResult();
    }

    private T handleResult() throws ExecutionException {
        if (remoteResult == null) {
            return null;
        }
        if (remoteResult.getException() != null) {
            throw new SpaceRemoteExecutionException(remotingEntry, remoteResult);
        }
        //noinspection unchecked
        return (T) remoteResult.getResult();
    }
}
