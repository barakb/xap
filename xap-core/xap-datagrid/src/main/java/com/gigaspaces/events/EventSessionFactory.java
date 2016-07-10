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

import java.rmi.RemoteException;

/**
 * the EventSessionFactory is a static service class that creates DataEventSession objects which are
 * used to intercept space data events. <br> <br> the EventSessionFactory is associated with a space
 * and is required in order to create a factory.<br> once a factory has been created, it can be used
 * to create sessions. <br> every session is configured according to an EventSessionConfig object.
 * <br> <br> the following code example shows how to create a session from a factory <br> and
 * register a listener to it. <br>
 *
 * <code> EventSessionConfig config = new EventSessionConfig(); <br> config.setFifo(true); <br>
 * config.setBatch(100, 20000); // size = 100 , time = 20000ms <br> <br> IJSpace space = ... <br>
 * EventSessionFactory factory = EventSessionFactory.getFactory(space); <br> DataEventSession
 * session = factory.newDataEventSession(config); <br> Object template = ... <br>
 * RemoteEventListener listener = ... <br> session.addListener(template, listener); <br> </code>
 *
 * @author asy ronen
 * @version 1.0
 * @see com.gigaspaces.events.DataEventSession
 * @see com.gigaspaces.events.EventSessionConfig
 * @since 6.0
 * @deprecated Since 9.7.0 - Use GigaSpace.newDataEventSession instead.
 */
@Deprecated

public class EventSessionFactory {

    private final IJSpace space;

    private EventSessionFactory(IJSpace space) {
        this.space = space;
    }

    /**
     * Retrieves the EventSessionFactory according to the space.
     *
     * @param space the associated space
     * @return the related factory.
     */
    public static EventSessionFactory getFactory(IJSpace space) {
        return new EventSessionFactory(space);
    }

    /**
     * creates a new {@link DataEventSession} using the specified space and default configuration.
     *
     * @param space The space which the session will listen to.
     * @return the newly created DataEventSession.
     * @throws RemoteException when connection to the space fails.
     * @since 8.0.4
     */
    public static DataEventSession newDataSession(IJSpace space) throws RemoteException {
        return getFactory(space).newDataEventSession();
    }

    /**
     * creates a new {@link DataEventSession} using the specified space and configuration.
     *
     * @param space  The space which the session will listen to.
     * @param config A set of configuration settings used to configure the session.
     * @return the newly created DataEventSession.
     * @throws RemoteException when connection to the space fails.
     * @since 8.0.4
     */
    public static DataEventSession newDataSession(IJSpace space, EventSessionConfig config) throws RemoteException {
        return getFactory(space).newDataEventSession(config);
    }

    /**
     * creates a new {@link DataEventSession} using the default configuration.
     *
     * @return the newly created DataEventSession.
     * @throws RemoteException when connection to the space fails.
     * @since 6.5
     */
    public DataEventSession newDataEventSession() throws RemoteException {
        return newDataEventSession(new EventSessionConfig());
    }

    /**
     * creates a new {@link DataEventSession} using the specified configuration.
     *
     * @param config A set of configuration settings used to configure the session.
     * @return the newly created DataEventSession.
     * @throws RemoteException when connection to the space fails.
     * @since 8.0.4
     */
    public DataEventSession newDataEventSession(EventSessionConfig config) throws RemoteException {
        return DataEventSessionFactory.create(space, config);
    }
}
