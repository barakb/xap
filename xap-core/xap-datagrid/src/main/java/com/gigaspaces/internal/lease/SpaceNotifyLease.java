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

package com.gigaspaces.internal.lease;

import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.ManagedRemoteEventListener;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.j_spaces.core.ObjectTypes;

import net.jini.core.event.EventRegistration;
import net.jini.core.lease.UnknownLeaseException;

import java.rmi.RemoteException;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceNotifyLease extends SpaceLease {
    private ManagedRemoteEventListener _listener;
    private GSEventRegistration _reg;

    public SpaceNotifyLease(IDirectSpaceProxy spaceProxy, String typeName, String uid, Object routingValue,
                            long expiration, ManagedRemoteEventListener listener, GSEventRegistration reg) throws RemoteException {
        super(spaceProxy, typeName, uid, routingValue, expiration);
        _listener = listener;
        _reg = reg;
        if (_listener != null)
            listener.init(reg);
    }

    @Override
    protected int getLeaseObjectType() {
        return ObjectTypes.NOTIFY_TEMPLATE;
    }

    @Override
    public void cancel() throws UnknownLeaseException, RemoteException {
        super.cancel();

        if (_listener != null)
            _listener.shutdown(_reg);
    }

    public EventRegistration getEventRegistration() {
        return _reg;
    }
}
