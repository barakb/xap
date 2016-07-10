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
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.lrmi.GenericExporter;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;

import java.lang.ref.WeakReference;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutorService;

/**
 * @author Niv Ingberg
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyDataEventListener extends DataEventListener {
    private final WeakReference<IJSpace> _space;
    private final GenericExporter _exporter;

    public SpaceProxyDataEventListener(RemoteEventListener listener, ExecutorService threadPool,
                                       IJSpace space, GenericExporter exporter) {
        super(listener, threadPool);
        _space = new WeakReference<IJSpace>(space);
        _exporter = exporter;
    }

    @Override
    public void shutdown(GSEventRegistration registration) {
        try {
            if (_exporter != null)
                _exporter.unexport(this);
        } catch (Exception e) {
            // already un-exported
        }

        super.shutdown(registration);
    }

    @Override
    public void notify(RemoteEvent event) throws UnknownEventException, RemoteException {
        IJSpace space = _space.get();
        if (space != null) {
            ((EntryArrivedRemoteEvent) event).setSpaceProxy(space);
            super.notify(event);
        }
    }

    @Override
    public void notifyBatch(BatchRemoteEvent batchEvent) throws UnknownEventException, RemoteException {
        IJSpace space = _space.get();
        if (space != null) {
            batchEvent.setSpaceProxy(space);
            super.notifyBatch(batchEvent);
        }
    }
}
