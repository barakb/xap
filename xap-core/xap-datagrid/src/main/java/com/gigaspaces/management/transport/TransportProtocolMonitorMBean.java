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

import java.rmi.NoSuchObjectException;
import java.util.List;


/**
 * This interface provides runtime monitoring of transport communication protocol.
 *
 * @author Igor Goldenberg
 * @since 6.0
 **/
public interface TransportProtocolMonitorMBean {
    /**
     * Returns the list of physical connections connected to the supplied remoteObjId.
     *
     * @param remoteObjID the remote objectID
     * @return list of physical connections connected to the remote object.
     * @throws NoSuchObjectException thrown an exception if the supplied remoteObjID is not found in
     *                               remote object registry. The reason might be a wrong objID
     *                               supplied or remote object was already unexported.
     **/
    public List<ITransportConnection> getTransportConnections(long remoteObjID)
            throws NoSuchObjectException;

    /**
     * Enables monitoring at the network layer, this will track each hosted service (i.e space,
     * processing unit) the invocation count and network traffic generated per remote method call
     * from each remote client invocation on this jvm. <note>Tracking may reduce impact
     * performance.</note>
     *
     * @since 9.1
     */
    void enableMonitoring();

    /**
     * Disabled monitoring at the network layer. {@link #enableMonitoring()}
     *
     * @since 9.1
     */
    void disableMonitoring();

    /**
     * Gets the network layer monitoring details, {@link #enableMonitoring()} must be called in
     * order to be able to get monitoring details.
     *
     * @since 9.1
     */
    String fetchMonitoringDetails();

}
