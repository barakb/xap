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

package com.gigaspaces.internal.server.space.events;

import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIUtilities;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.UnknownEventException;

import java.rmi.RemoteException;

/**
 * A RemoteEventBatchBusPacket represents a batch of remote events that should be sent to a remote
 * listener.
 *
 * @author Asy
 */
@com.gigaspaces.api.InternalApi
public class RemoteEventBatchBusPacket extends RemoteEventBusPacket {
    private BatchRemoteEvent _remoteEvents;

    /**
     * Construct a mobile remote event instance, by instantiating a bus packet and attaching it a
     * remote event.
     */
    public RemoteEventBatchBusPacket(ITemplateHolder templateHolder, RemoteEvent[] events) {
        super(templateHolder, null, 0, null, false);
        this._remoteEvents = new BatchRemoteEvent(events);
    }


    public BatchRemoteEvent getRemoteEvents() {
        return _remoteEvents;
    }


    @Override
    public void notifyListener() throws RemoteException, UnknownEventException {
        NotifyTemplateHolder template = (NotifyTemplateHolder) getEntryHolder();

        BatchRemoteEventListener listener = (BatchRemoteEventListener) template.getREListener();
        if (listener != null) {
            //Listener could be changed to null concurrently by a different thread that decided to disconnect
            //this listener because it received an exception while dispatching event which belong to this template
            if (LRMIUtilities.isRemoteProxy(listener))
                LRMIInvocationContext.enableCustomPriorityForNextInvocation();
            listener.notifyBatch(getRemoteEvents());
        }

    }


}
