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

package com.j_spaces.core.client;

import com.gigaspaces.events.AbstractDataEventSession;
import com.gigaspaces.events.DataEventSessionFactory;
import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.NotifyInfo;
import com.j_spaces.core.IJSpace;

import java.rmi.RemoteException;

/**
 * @deprecated Use {@link com.gigaspaces.events.DataEventSession} instead.
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class NotifyDelegator {
    private final AbstractDataEventSession _eventSession;
    private final GSEventRegistration _eventRegistration;

    public NotifyDelegator(IJSpace space, Object template, long lease, NotifyInfo info)
            throws RemoteException {
        _eventSession = (AbstractDataEventSession) DataEventSessionFactory.create(space, new EventSessionConfig()
                .setComType(EventSessionConfig.ComType.UNICAST));
        _eventRegistration = (GSEventRegistration) _eventSession.addListener(template, lease, info);
    }

    public GSEventRegistration getEventRegistration() {
        return _eventRegistration;
    }

    public void close() throws RemoteException {
        _eventSession.close();
    }
}
