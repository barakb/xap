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

package com.gigaspaces.internal.client.spaceproxy.events;

import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.fifo.BlockedOrderedQueue;
import com.j_spaces.kernel.SystemProperties;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;

import java.rmi.RemoteException;
import java.util.concurrent.ExecutorService;

/**
 * this is an event listener wrapper that delivers FIFO notifications to the inner listener <br> the
 * order is kept per source (space). <br> no global order between all spaces is kept. <br>
 *
 * @author asy ronen
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class FifoDataEventListener extends DataEventListener {
    private static final int _queueSize = Integer.getInteger(SystemProperties.NOTIFY_FIFO_QUEUE, Integer.MAX_VALUE).intValue();

    private final BlockedOrderedQueue _fifoQueue;

    public FifoDataEventListener(RemoteEventListener listener,
                                 ExecutorService threadPool, NotifyInfo info) {
        super(listener, threadPool);
        _fifoQueue = new BlockedOrderedQueue(listener, threadPool, _queueSize, info);
    }

    @Override
    public void notify(RemoteEvent event) throws UnknownEventException, RemoteException {
        _fifoQueue.enqueue(event);
    }

    @Override
    public void notifyBatch(BatchRemoteEvent batchEvent) throws UnknownEventException, RemoteException {
        for (RemoteEvent event : batchEvent.getEvents())
            _fifoQueue.enqueue(event);
    }

    @Override
    public void init(GSEventRegistration registration) {
        super.init(registration);
        synchronized (this) {
            _fifoQueue.setInitialSeqNumbers(registration.getSequenceNumbers());
        }
    }

    @Override
    public void shutdown(GSEventRegistration registration) {
        _fifoQueue.interrupt();
        super.shutdown(registration);
    }
}
