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
package net.jini.core.lookup;

import net.jini.core.event.EventRegistration;
import net.jini.core.lease.Lease;

import java.io.Serializable;
import java.rmi.RemoteException;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class RegistrarEventRegistration extends EventRegistration implements Serializable {
    private static final long serialVersionUID = -1755524540939685862L;

    protected ServiceMatchesWrapper matches;

    public RegistrarEventRegistration(long eventID, Object source, Lease lease, long seqNum, ServiceMatchesWrapper matches) {
        super(eventID, source, lease, seqNum);
        this.matches = matches;
    }

    public ServiceMatches getMatches() throws RemoteException {
        return matches.get();
    }

    public void setMatches(ServiceMatchesWrapper matches) {
        this.matches = matches;
    }
}
