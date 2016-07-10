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

package com.j_spaces.kernel.weaklistener;

import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;

import java.lang.ref.WeakReference;
import java.rmi.RemoteException;

/**
 * Used in order to weakly hold batch listeners by the Notify delegator.
 *
 * @author anna
 * @version 1.0
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class WeakBatchRemoteEventListener extends WeakReference<BatchRemoteEventListener>
        implements BatchRemoteEventListener {
    public WeakBatchRemoteEventListener(BatchRemoteEventListener listener) {
        super(listener);
    }

    /**
     * Delegate the notification to the listener.
     */
    public void notify(RemoteEvent event) throws UnknownEventException, RemoteException {
        RemoteEventListener l = get();
        if (l != null) {
            l.notify(event);
        }
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.events.batching.BatchRemoteEventListener#notifyBatch(com.gigaspaces.events.batching.BatchRemoteEvent)
     */
    public void notifyBatch(BatchRemoteEvent theEvents)
            throws UnknownEventException, RemoteException {
        BatchRemoteEventListener l = get();
        if (l != null) {
            l.notifyBatch(theEvents);
        }
    }
}
