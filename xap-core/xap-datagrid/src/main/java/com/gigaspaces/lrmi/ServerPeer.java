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

package com.gigaspaces.lrmi;

import com.gigaspaces.config.lrmi.ITransportConfig;

import java.rmi.RemoteException;


/**
 * ServerPeer extends Peer with server-specific operations.
 *
 * An instance of a ServerPeer represents the binding of a remote object to the protocol abstracted
 * by the peer's originating protocol adapter.
 *
 * The peer instance is usually constructed by the protocol adapter getServerPeer() method, where it
 * also receives the remote object id.
 *
 * The beforeExport() and afterUnexport() methods enable the peer to synchronize its behavior, and
 * can also server as a hint for the protocol adapter to open new physical connections or close
 * existing ones.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
public interface ServerPeer extends Peer {
    /**
     * Returns the object id of the remote object that this peer serves.
     *
     * @return the object id of the remote object that this peer serves.
     */
    long getObjectId();

    long getObjectClassLoaderId();

    /**
     * Provides a chance to this peer to do initialization before the remote object it serves is
     * exported.
     *
     * @param props the properties provided by the export caller
     */
    void beforeExport(ITransportConfig config) throws RemoteException;

    /**
     * Provides a chance to this peer to do initialization after the remote object it serves is
     * unexported.
     *
     * @param force if <code>, the object has been unexported even if it was in the middle of
     *              serving client requests
     */
    void afterUnexport(boolean force) throws RemoteException;

    /**
     * Returns a connection URL for the remote object represented by this peer.
     *
     * @return a connection URL for the remote object represented by this peer.
     */
    String getConnectionURL();

}