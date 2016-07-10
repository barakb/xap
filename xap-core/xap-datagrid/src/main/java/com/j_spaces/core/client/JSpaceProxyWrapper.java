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

/*
 * @(#)JSpaceProxyWrapper.java 1.0   01/25/2001  17:15PM
 */

package com.j_spaces.core.client;

import com.j_spaces.core.IJSpace;
import com.j_spaces.core.JSpaceState;

import net.jini.core.lookup.ServiceID;
import net.jini.id.ReferentUuids;
import net.jini.id.Uuid;

import java.io.Serializable;
import java.rmi.Remote;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class JSpaceProxyWrapper implements Remote, Serializable {
    private static final long serialVersionUID = 1L;
    public static final short SPACE_MODE_REGULAR = 1;
    public static final short SPACE_MODE_PRIMARY = 2;
    public static final short SPACE_MODE_BACKUP = 3;
    public static final short SPACE_MODE_REMOVED_FROM_JINI_LUS = 4;

    private Object proxyObject;
    private String proxyName;

    /**
     * current space status, used in space browser to represent appropriately the tree icon
     */
    private int spaceState = JSpaceState.STARTED;
    private short _spaceMode = SPACE_MODE_REGULAR;
    private ServiceID _serviceID;

    public JSpaceProxyWrapper() {
    }

    public JSpaceProxyWrapper(IJSpace spaceProxy) {
        proxyObject = spaceProxy;
        proxyName = spaceProxy.getName();
        Uuid spaceUuid = spaceProxy.getReferentUuid();
        _serviceID = new ServiceID(spaceUuid.getMostSignificantBits(), spaceUuid.getLeastSignificantBits());
    }

    /**
     * Return the object that contains the JSpaceProxyWrapper. If the object stored as
     * MarshalledObject, so returns the unMarshalling object. get
     *
     * @return The object of JSpaceProxyWrapper
     **/
    public Object proxy() {
        return proxyObject;
    }

    @Override
    public String toString() {
        return proxyName;
    }

    public String getProxyName() {
        return proxyName;
    }

    /**
     * Proxies for servers with the same uuid have the same hash code.
     */
    @Override
    public int hashCode() {
        return (proxyObject.hashCode());
    }

    /**
     * Proxies for servers with the same Uuid are considered equal.
     */
    @Override
    public boolean equals(Object o) {
        return ReferentUuids.compare(proxyObject, o);
    }

    public int getState() {
        return spaceState;
    }

    public void setState(int state) {
        spaceState = state;
    }

    /**
     * This method returns ServiceID instance of space or container.
     *
     * @return ServiceID instance of space or container
     */
    public ServiceID getServiceID() {
        return _serviceID;
    }

    public short getSpaceMode() {
        return _spaceMode;
    }

    public void setSpaceMode(short mode) {
        _spaceMode = mode;
    }
}