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

package com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.processlog.ReplicationConsumeTimeoutException;
import com.gigaspaces.time.SystemTime;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


@com.gigaspaces.api.InternalApi
public class ParallelBatchProcessingContext {

    private final AtomicInteger _processedCount = new AtomicInteger(0);
    private Throwable _errorResult;
    private final List<IReplicationOrderedPacket>[] _parallelLists;
    private final int _parallelParticipants;

    public ParallelBatchProcessingContext(
            List<IReplicationOrderedPacket>[] parallelLists,
            int parallelParticipants) {
        _parallelLists = parallelLists;
        _parallelParticipants = parallelParticipants;
    }

    public synchronized void waitForCompletion(long timeout, TimeUnit units)
            throws Throwable {
        long startTime = SystemTime.timeMillis();
        while (_processedCount.get() < _parallelParticipants
                && _errorResult == null) {
            long currentTime = SystemTime.timeMillis();
            if (currentTime - startTime > units.toMillis(timeout)) {
                throw new ReplicationConsumeTimeoutException("Timeout exceeded ["
                        + timeout
                        + "ms] while waiting for batch to be consumed");
            }
            wait(units.toMillis(timeout));
        }
        if (_errorResult != null)
            throw _errorResult;
    }

    public void signalOneProcessedOk() {
        if (_processedCount.incrementAndGet() == _parallelParticipants) {
            synchronized (this) {
                notifyAll();
            }
        }
    }

    public synchronized void setError(Throwable error) {
        _errorResult = error;
        notifyAll();
    }

    public List<IReplicationOrderedPacket> getSegment(int segmentIndex) {
        return _parallelLists[segmentIndex];
    }

}
