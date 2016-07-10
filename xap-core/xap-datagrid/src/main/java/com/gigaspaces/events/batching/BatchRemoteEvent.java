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

package com.gigaspaces.events.batching;

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;

import net.jini.core.event.RemoteEvent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Received on {@link BatchRemoteEventListener#notifyBatch(BatchRemoteEvent)} when a batch of events
 * that matches the pending templates registration.
 *
 * @author Assy
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class BatchRemoteEvent extends EntryArrivedRemoteEvent {
    private static final long serialVersionUID = 224729391815755896L;

    private RemoteEvent[] _events;

    public BatchRemoteEvent() {
    }

    public BatchRemoteEvent(RemoteEvent[] events) {
        this._events = events;
        EntryArrivedRemoteEvent firstEvent = (EntryArrivedRemoteEvent) events[0];
        this._templateUID = firstEvent.getTemplateUID();
        this.eventID = firstEvent.getID();
        setSpaceProxyUuid(firstEvent.getSpaceUuid());
    }

    /**
     * @return an array of remote events
     */
    public RemoteEvent[] getEvents() {
        return _events;
    }


    /**
     * Used internally to set the proxy for all the events
     */
    @Override
    public void setSpaceProxy(IJSpace spaceProxy) {
        _spaceProxy = spaceProxy;

        for (int i = 0; i < _events.length; i++) {
            EntryArrivedRemoteEvent event = ((EntryArrivedRemoteEvent) _events[i]);
            event.setSpaceProxy(spaceProxy);
        }
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);

        if (_events != null) {
            out.writeInt(_events.length);
            for (RemoteEvent event : _events)
                out.writeObject(event);
        } else
            out.writeInt(-1);
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        int size = in.readInt();
        if (size > -1) {
            _events = new RemoteEvent[size];
            for (int i = 0; i < size; i++)
                _events[i] = (RemoteEvent) in.readObject();
        }
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "notify batch";
    }
}
