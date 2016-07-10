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

package com.gigaspaces.internal.cluster.node.impl.groups;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@com.gigaspaces.api.InternalApi
public class ReplicateFuture
        implements Future {

    private boolean _releasedOk;
    private Throwable _error;

    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return _releasedOk || _error != null;
    }

    public Object get() throws InterruptedException, ExecutionException {
        synchronized (this) {
            while (!isDone())
                wait();
        }
        if (_releasedOk)
            return null;
        throw new ExecutionException(_error);
    }

    public Object get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        return get();
    }

    public synchronized void releaseOk() {
        _releasedOk = true;
        notifyAll();
    }

    public synchronized void releaseError(Throwable t) {
        _error = t;
        notifyAll();
    }

}
