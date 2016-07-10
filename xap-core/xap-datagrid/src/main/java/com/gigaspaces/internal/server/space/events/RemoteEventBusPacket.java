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

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;
import com.j_spaces.core.server.processor.BusPacket;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;

import java.rmi.RemoteException;

/**
 * A RemoteEventBusPacket represents a remote event that should be sent to a remote listener.
 */
@com.gigaspaces.api.InternalApi
public class RemoteEventBusPacket extends BusPacket<SpaceDataEventDispatcher> {
    private final RemoteEvent _remoteEvent;
    private int _TTL;
    private final NotifyContext _notifyContext;
    private boolean _afterBatching = false;
    private final boolean _fromReplication;

    /**
     * Construct a mobile remote event instance, by instantiating a bus packet and attaching it a
     * remote event.
     */
    public RemoteEventBusPacket(ITemplateHolder templateHolder, RemoteEvent re,
                                int TTL, NotifyContext notifyContext, boolean fromReplication) {
        super((OperationID) null, templateHolder, null, 0);

        // Workaround to make sure the bus-packet OperationID will be set.
        if (re instanceof EntryArrivedRemoteEvent)
            super.setOperationID(((EntryArrivedRemoteEvent) re).getOperationID());

        _remoteEvent = re;
        _TTL = TTL;
        _notifyContext = notifyContext;
        _fromReplication = fromReplication;
    }

    public void afterBatching() {
        this._afterBatching = true;
    }

    public boolean isAfterBatching() {
        return _afterBatching;
    }


    /**
     * @return the remoteEvent
     */
    public RemoteEvent getRemoteEvent() {
        return _remoteEvent;
    }


    /**
     * @param tTL the tTL to set
     */
    public void setTTL(int tTL) {
        _TTL = tTL;
    }


    /**
     * @return the tTL
     */
    public int getTTL() {
        return _TTL;
    }


    /**
     * @return the status
     */
    public NotifyContext getStatus() {
        return _notifyContext;
    }

    @Override
    public void execute(SpaceDataEventDispatcher dispatcher) throws Exception {
        dispatcher.execute(this);
    }


    /**
     * Send notification to the client
     */
    public void notifyListener() throws RemoteException, UnknownEventException {
        NotifyTemplateHolder template = (NotifyTemplateHolder) getEntryHolder();

        RemoteEventListener listener = template.getREListener();

        if (listener != null) {
            //Listener could be changed to null concurrently by a different thread that decided to disconnect
            //this listener because it received an exception while dispatching event which belong to this template
            if (LRMIUtilities.isRemoteProxy(listener))
                LRMIInvocationContext.enableCustomPriorityForNextInvocation();
            listener.notify(getRemoteEvent());

        }

    }


    /**
     * @return
     */
    public boolean isFromReplication() {
        return _fromReplication;
    }
}