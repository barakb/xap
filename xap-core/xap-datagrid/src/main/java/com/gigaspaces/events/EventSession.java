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


package com.gigaspaces.events;

import com.j_spaces.core.IJSpace;

import net.jini.core.lease.UnknownLeaseException;

import java.rmi.RemoteException;

/**
 * a common interface for all EventSession types <br> a session is a stateful service used to
 * register event listeners to the space <br> the state of the session contains a set of
 * configuration parameters such as the communication protocol <br> used to convey the events and
 * whether of not to use FIFO order <br> the session can be used to register multiple listeners that
 * share the same configuration. <br> the session provides methods for retrieving the bounded <br>
 * entities of the session - the transaction and the configuration object. <br> the interface also
 * provides a lifecycle management support methods.
 *
 * @author asy ronen
 * @version 1.0
 * @see com.gigaspaces.events.EventSessionConfig
 * @since 6.0
 */
public interface EventSession {

    /**
     * Retrieves the bounded configuration object.
     *
     * @return the EventSessionConfig object.
     */
    EventSessionConfig getSessionConfig();

    /**
     * @return the space that the session is attached to.
     */
    IJSpace getSpace();

    /**
     * closes the session and deregister all listeners.
     *
     * @throws RemoteException       if the remove space failed to remove the notification
     *                               registration.
     * @throws UnknownLeaseException if the listeners lease has expired.
     */
    void close() throws RemoteException, UnknownLeaseException;
}
