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

package com.gigaspaces.management.transport;

import java.io.Serializable;
import java.util.Date;

/**
 * This is a super class of transport protocol provider with all related information to implemented
 * protocol.
 *
 * @author Igor Goldenberg
 * @since 6.0
 */
public interface ITransportConnection
        extends Serializable {
    /**
     * @return the remoteObjectID of this transport connection.
     */
    public long getRemoteObjID();

    /**
     * @return the client connection time to the remoteObject.
     */
    public Date getConnectTime();

    /**
     * @return the client IP address which is connected to the {@link #getRemoteObjID()}
     */
    public String getClientIPAddress();

    /**
     * @return the client port.
     */
    public int getClientPort();

    /**
     * @return the server IP address the client connect to.
     */
    public String getServerIPAddress();

    /**
     * @return the server port.
     */
    public int getServerPort();

    /**
     * @return the unique connection ID identifier
     */
    public long getConnectionID();
}
