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

import com.j_spaces.core.client.INotifyDelegatorFilter;

import net.jini.core.event.EventRegistration;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.lease.UnknownLeaseException;

import java.rmi.MarshalledObject;
import java.rmi.RemoteException;

/**
 * The common interface that is used when working with data events.<br> listeners can be registered
 * using the addListener() methods and receive <br> an EventRegistration that will be used to
 * unregister using the removeListener(). <br>
 *
 * @author asy ronen
 * @version 1.0
 * @see com.gigaspaces.events.EventSession
 * @since 6.0
 */
public interface DataEventSession extends EventSession {

    /**
     * Registers a listener with a space using a POJO template. Equals to addListener( template,
     * listener, Lease.FOREVER, NotifyActionType.NOTIFY_ALL)
     *
     * @param template the template to be match with.
     * @param listener the listener
     * @return the EventRegistration object.
     * @throws RemoteException if the space has failed to perform the registration
     */
    EventRegistration addListener(Object template, RemoteEventListener listener)
            throws RemoteException;

    /**
     * Registers a listener with a space using a POJO template
     *
     * @param template    the template to be match with.
     * @param listener    the listener
     * @param actionTypes the type of actions you wish to receive notification on.
     * @return the EventRegistration object.
     * @throws RemoteException if the space has failed to perform the registration
     */
    EventRegistration addListener(Object template, RemoteEventListener listener, NotifyActionType actionTypes)
            throws RemoteException;

    /**
     * Registers a listener with a space using a POJO template
     *
     * @param template    the template to be match with.
     * @param listener    the listener
     * @param actionTypes the type of actions you wish to receive notification on.
     * @param handback    an object that will be passed back to the listener when notified.
     * @param filter      a server-side filter to filter out notifications.
     * @return the EventRegistration object.
     * @throws RemoteException if the space has failed to perform the registration
     */
    EventRegistration addListener(Object template, RemoteEventListener listener, NotifyActionType actionTypes,
                                  MarshalledObject handback, INotifyDelegatorFilter filter)
            throws RemoteException;

    /**
     * Data event registration with custom lease is deprecated - use {@link #addListener(Object,
     * net.jini.core.event.RemoteEventListener, NotifyActionType)} instead.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    EventRegistration addListener(Object template, RemoteEventListener listener, long lease,
                                  NotifyActionType actionTypes)
            throws RemoteException;

    /**
     * Data event registration with custom lease is deprecated - use {@link #addListener(Object,
     * net.jini.core.event.RemoteEventListener, NotifyActionType, java.rmi.MarshalledObject,
     * com.j_spaces.core.client.INotifyDelegatorFilter)} instead.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    EventRegistration addListener(Object template, RemoteEventListener listener, long lease,
                                  MarshalledObject handback, INotifyDelegatorFilter filter,
                                  NotifyActionType actionTypes)
            throws RemoteException;

    /**
     * Unregisters the listener from the space.
     *
     * @param registration the registration object received from the server when the listener was
     *                     added.
     * @throws RemoteException       if the space has failed to unregister the listener.
     * @throws UnknownLeaseException if the lease of the registration is invalid.
     */
    void removeListener(EventRegistration registration)
            throws RemoteException, UnknownLeaseException;
}
