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
import com.gigaspaces.exception.lrmi.ApplicationException;
import com.gigaspaces.exception.lrmi.ProtocolException;

import java.net.MalformedURLException;
import java.rmi.RemoteException;

/**
 * ClientPeer extends Peer with client-specific operations. <p/> A client peer is maintaining a
 * logical connection with a remote object with a unique id. The client peer usually connects to the
 * server peer, invokes operations on the remote object using the server peer, and disconnects. <p/>
 * A pair of getter/setter methods control connection retries. <p/> The connection itself is URL
 * based. The connection URL is protocol adapter specific, but it usually has the following format:
 * <p/> <protocol>://<serverhost>:<port>/<remote-object-id-string> <p/> A client can obtain the
 * connection URL from a directory service, via e-mail or in some other way. The client can save the
 * connection URL and connect to the same logical remote object at a later time.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
public interface ClientPeer extends Peer {
    void init(ITransportConfig config);

    /**
     * Connects to the remote server peer.
     *
     * @param connectionURL usually of the form <protocol>://<serverhost>:<port>/<remote-object-id-string>
     * @throws java.net.MalformedURLException if the URL is not understood by the implementation
     * @RemoteException if the connection fails.
     */
    public void connect(String connectionURL, LRMIMethod lrmiMethod)
            throws MalformedURLException, RemoteException;

    /**
     * Returns the connection URL associated with this peer.
     *
     * @return the connection URL associated with this peer.
     */
    public String getConnectionURL();


    /**
     * Returns the number of connection retries associated with this peer.
     *
     * @return the number of connection retries associated with this peer.
     */
    public int getConnectRetries();

    /**
     * Sets the number of connection retries associated with this peer.
     */
    public void setConnectRetries(int connectRetries);

    /**
     * Invokes a method on the remote object this peer is connected to. This method should propagate
     * the methodName, arguments and security context associated with this peer (if exists), in a
     * protocol specific format to the remote server peer, and return the result to the caller.
     *
     * @param lrmiMethod contains necessary information about invocation remote call
     * @param args       arguments to the method
     * @throws ProtocolException a result of a remote method invocation is thrown while
     *                           unmarshalling the arguments or de/serialization request/reply
     *                           packet.
     * @throws RemoteException   failed to open/close connection with remote endpoint.
     */
    public Object invoke(Object proxy, LRMIMethod lrmiMethod, Object[] args, ConnectionPool connPool)
            throws ApplicationException, ProtocolException, RemoteException, InterruptedException;

    public long getObjectId();

    public void setObjectId(long objectId);

    /**
     * Send keep alive request to server
     *
     * @return <tt>true</tt> if keep alive was sent successfully
     */
    public boolean sendKeepAlive();
}