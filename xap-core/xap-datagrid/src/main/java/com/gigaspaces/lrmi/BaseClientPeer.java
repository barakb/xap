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

import java.net.MalformedURLException;
import java.rmi.RemoteException;

/**
 * BaseClientPeer is a convenient abstract superclass for Client Peer classes of Protocol Adapters.
 * Concrete subclasses should at least implement connect(), disconnect() and invoke().
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
public abstract class BaseClientPeer
        extends ConnectionResource {
    final static int DEFAULT_CONN_RETRIES = 3;

    private ProtocolAdapter _protocolAdapter;
    private String _connectionURL;
    private volatile boolean _connected;
    private volatile int _connectRetries;
    private long _objectId;

    /**
     * Constructor.
     *
     * @param protocolAdapter Client's peer adaptor.
     */
    public BaseClientPeer(ProtocolAdapter protocolAdapter) {
        _protocolAdapter = protocolAdapter;
        _connectRetries = DEFAULT_CONN_RETRIES;
    }

    public boolean isConnected() {
        return _connected;
    }

    public void setConnected(boolean connected) {
        // volatile modification!
        _connected = connected;
    }

    public ProtocolAdapter getProtocolAdapter() {
        return _protocolAdapter;
    }

    public String getConnectionURL() {
        return _connectionURL;
    }

    public void setConnectionURL(String connectionURL) throws MalformedURLException, RemoteException {
        _connectionURL = connectionURL;
    }

    public int getConnectRetries() {
        return _connectRetries;
    }

    public void setConnectRetries(int connectRetries) {
        //volatile modification!
        _connectRetries = connectRetries;
    }

    public long getObjectId() {
        return _objectId;
    }

    public void setObjectId(long objectId) {
        _objectId = objectId;
    }

    @Override
    protected void finalize() throws Throwable {
        disconnect();
        super.finalize();
    }
}