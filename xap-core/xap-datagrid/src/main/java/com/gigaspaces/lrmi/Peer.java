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


/**
 * Peer is an abstraction for a connection endpoint, similar to a socket in the TCP/IP world.
 *
 * Peer is extended by the ClientPeer and ServerPeer interfaces.
 *
 * Each LRMI logical connection is abstracted by a pair of ClientPeer and ServerPeer. Physical
 * connections may be shared (this is implementation dependent).
 *
 * Each connection has a timeout attribute, for network oriented operations (connect, invoke,
 * read).
 *
 * Each connection may have a security context property. A security context is an object that may be
 * passed by the protocol adapter provider from a client peer to a server peer in an invocation. A
 * client peer usually sets the security context of its peer, and the server can retreieve it.
 * Sophisticated protocol adapter implementations can use a caching mechanism of the security
 * context on the server peer, forwarding a new security context only upon change of the client
 * security context.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
public interface Peer {
    /**
     * Checks if the current peer is in 'connected' state.
     *
     * @return <code>true</code> if connected, <code>false</code> otherwise.
     */
    boolean isConnected();

    /**
     * Disconnects, effectively closes the connection.
     *
     * @throws java.rmi.RemoteException if there was a problem with closing the connection.
     */
    void disconnect();

    /**
     * Returns the protocol adapter that created this peer.
     *
     * @return the protocol adapter that created this peer.
     */
    <C extends ClientPeer> ProtocolAdapter<C> getProtocolAdapter();
}