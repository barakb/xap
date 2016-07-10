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

package com.gigaspaces.lrmi.nio;

import com.gigaspaces.management.transport.ITransportConnection;

import java.util.Date;


/**
 * This class provides full information about physical connection to the remote object.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @since 6.0
 **/
@com.gigaspaces.api.InternalApi
public class NIOTransportConnection
        implements ITransportConnection {
    private static final long serialVersionUID = 1L;

    final static String TRANSPORT_PROTOCOL_NAME = "NIO";

    /**
     * the unique remote objectID
     */
    long remoteObjID;

    /**
     * unique connection ID identifier
     */
    long connectionID;

    /**
     * connection time to the remoteObj
     */
    Date connectTime;

    String serverIPAddress;
    int serverPort;

    String clientIPAddress;
    int clientPort;

//  List< InvokedMethodInfo> invokedMethods; 

    /**
     * Constructor.
     **/
    public NIOTransportConnection(long remoteObjID, long connectionID, long connectTime,
                                  String clientIPAddress, int clientPort,
                                  String serverIPAddress, int serverPort) {
        this.remoteObjID = remoteObjID;
        this.connectionID = connectionID;
        this.connectTime = new Date(connectTime);
        this.clientIPAddress = clientIPAddress;
        this.clientPort = clientPort;
        this.serverIPAddress = serverIPAddress;
        this.serverPort = serverPort;
    }

    /**
     * @see com.gigaspaces.management.transport.ITransportConnection#getRemoteObjID()
     */
    public long getRemoteObjID() {
        return remoteObjID;
    }

    /**
     * @see com.gigaspaces.management.transport.ITransportConnection#getConnectTime()
     */
    public Date getConnectTime() {
        return connectTime;
    }

    /**
     * @see com.gigaspaces.management.transport.ITransportConnection#getServerIPAddress()
     */
    public String getServerIPAddress() {
        return serverIPAddress;
    }

    /**
     * @see com.gigaspaces.management.transport.ITransportConnection#getClientIPAddress()
     */
    public String getClientIPAddress() {
        return clientIPAddress;
    }

    /**
     * @see com.gigaspaces.management.transport.ITransportConnection#getClientPort()
     */
    public int getClientPort() {
        return clientPort;
    }

    /**
     * @see com.gigaspaces.management.transport.ITransportConnection#getServerPort()
     */
    public int getServerPort() {
        return serverPort;
    }

    /**
     * @see com.gigaspaces.management.transport.ITransportConnection#getConnectionID()
     */
    public long getConnectionID() {
        return connectionID;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("NIO Transport Protocol info:");
        sb.append("\nRemoteObjectID: " + getRemoteObjID());
        sb.append("\nConnectionID: " + getConnectionID());
        sb.append("\nConnectionTime: " + getConnectTime());
        sb.append("\nClient IP Address: " + getClientIPAddress() + ":" + getClientPort());
        sb.append("\nServer IP Address: " + getServerIPAddress() + ":" + getClientPort());

        return sb.toString();
    }
}