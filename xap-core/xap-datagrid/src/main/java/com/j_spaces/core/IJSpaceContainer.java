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


package com.j_spaces.core;

import com.j_spaces.core.client.SpaceURL;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author Igor Goldenberg
 * @version 5.1
 * @see com.j_spaces.core.IJSpace
 * @see com.j_spaces.core.client.SpaceFinder
 * @since 1.0
 * @deprecated Since 8.0 - This interface is reserved for internal usage only.
 **/
@Deprecated
public interface IJSpaceContainer extends Remote {
    /**
     * Returns the name of this container.
     *
     * @return the name of this container.
     * @throws java.rmi.RemoteException if a communication error occurs
     **/
    String getName() throws RemoteException;

    /**
     * Returns the {@link SpaceURL} instance which was used to initialize the space. <p> Notice: The
     * {@link SpaceURL} object contains information on the space and container configuration/setup
     * such as space url used, space/container/cluster schema used and other attributes.<p> The
     * {@link IJSpace} keeps also its reference of the SpaceURL which launched the container.
     *
     * @return {@link SpaceURL} which initialized that specific space instance.
     **/
    SpaceURL getURL() throws RemoteException;

    /**
     * Checks whether the container is alive and accessible.
     * <pre><code>
     * IJSpaceContainer aContainer = (IJSpaceContainer){@link com.j_spaces.core.client.SpaceFinder#find(String)
     * SpaceFinder.find("jini://lookup-host/container-name")};
     * try{
     *    aContainer.ping();
     *    System.out.println("Container alive");
     * }
     * catch (java.rmi.RemoteException re) {
     *    System.out.println("Container unreachable");
     * }
     * </code></pre>
     *
     * @throws java.rmi.RemoteException when container was unreachable
     */
    void ping() throws RemoteException;

    /**
     * Returns the names of the spaces that belong to this container.
     *
     * @return String[]  the names of the spaces that belong to this container.
     * @throws java.rmi.RemoteException if a communication error occurs
     **/
    @Deprecated
    String[] getSpaceNames() throws RemoteException;

    /**
     * Returns a proxy of the specified space.
     *
     * @param spaceName the name of the space.
     * @return IJSpace space proxy.
     * @throws NoSuchNameException      if the specified space does not exist in this container.
     * @throws java.rmi.RemoteException if a communication error occurs
     **/
    @Deprecated
    IJSpace getSpace(String spaceName)
            throws NoSuchNameException, RemoteException;

    /**
     * Returns a clustered proxy of the specified space.
     *
     * @param spaceName the name of the space.
     * @return IJSpace clustered space proxy.
     * @throws NoSuchNameException      if the specified space does not exist in this container.
     * @throws java.rmi.RemoteException if a communication error occurs
     **/
    @Deprecated
    IJSpace getClusteredSpace(String spaceName)
            throws NoSuchNameException, RemoteException;

    /**
     * Shuts down this container. This also involves unregistering all registered spaces from Lookup
     * services and closing connections to storage adapters.
     *
     * @throws java.rmi.RemoteException if a communication error occurs
     **/
    @Deprecated
    void shutdown() throws RemoteException;
}
