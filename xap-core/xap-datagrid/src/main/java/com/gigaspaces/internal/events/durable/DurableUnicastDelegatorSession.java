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

package com.gigaspaces.internal.events.durable;

import com.gigaspaces.events.AbstractDataEventSession;
import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.events.IInternalEventSessionAdmin;
import com.j_spaces.core.IJSpace;

import net.jini.core.event.EventRegistration;
import net.jini.core.lease.Lease;
import net.jini.id.Uuid;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Dan Kilman
 * @since 9.0
 */
public final class DurableUnicastDelegatorSession extends AbstractDataEventSession
        implements IInternalEventSessionAdmin {
    private final Map<EventRegistration, ReplicationNotificationClientEndpoint> _notifications;

    public DurableUnicastDelegatorSession(IJSpace space, com.gigaspaces.events.EventSessionConfig config) {
        super(space, config);
        this._notifications = new ConcurrentHashMap<EventRegistration, ReplicationNotificationClientEndpoint>();
    }

    @Override
    protected EventRegistration addListenerInternal(Object template, long lease, NotifyInfo info)
            throws RemoteException {
        ReplicationNotificationClientEndpoint notification;
        EventRegistration eventRegistration = null;

        notification = new ReplicationNotificationClientEndpoint(getNotificationsSpace(),
                template,
                info,
                getSessionConfig(),
                lease);

        long eventID = notification.getEventID();
        String templateUID = info.getTemplateUID();
        Uuid spaceUID = notification.getSpaceUID();
        Lease durableLease = notification.getLease();

        eventRegistration = new GSEventRegistration(eventID,
                null /* source */,
                durableLease /* lease */,
                0 /* sequence number */,
                templateUID,
                spaceUID);

        _notifications.put(eventRegistration, notification);

        return eventRegistration;
    }

    @Override
    public void removeListener(EventRegistration registration)
            throws RemoteException {
        ReplicationNotificationClientEndpoint endpoint = _notifications.remove(registration);
        if (endpoint != null)
            endpoint.close();
    }

    @Override
    public void close() throws RemoteException {
        for (ReplicationNotificationClientEndpoint endpoint : _notifications.values())
            endpoint.close();

        _notifications.clear();
    }

    @Override
    public String dumpState(EventRegistration registration) {
        ReplicationNotificationClientEndpoint endpoint = _notifications.get(registration);
        if (endpoint == null)
            return "No such registration";

        return endpoint.getNotificationReplicationNode().getAdmin().dumpState();
    }
}
