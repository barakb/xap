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

import com.gigaspaces.lrmi.RemoteStub;
import com.j_spaces.core.admin.ContainerConfig;
import com.j_spaces.core.admin.IJSpaceContainerAdmin;
import com.j_spaces.core.client.SpaceURL;

import java.nio.channels.ClosedChannelException;
import java.rmi.ConnectException;
import java.rmi.RemoteException;

/**
 * LRMI Stub implementation of <code>IJSpaceContainer, IJSpaceContainerAdmin,
 * LifeCycle,Service</code> interface.
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class LRMIJSpaceContainer extends RemoteStub<IJSpaceContainer>
        implements IJSpaceContainer, IJSpaceContainerAdmin {
    static final long serialVersionUID = 1L;

    private String _name;

    public LRMIJSpaceContainer() {
    }

    public LRMIJSpaceContainer(IJSpaceContainer directObjRef, IJSpaceContainer dynamicProxy, String name) {
        super(directObjRef, dynamicProxy);
        this._name = name;
    }

    public IJSpace getClusteredSpace(String spaceName)
            throws NoSuchNameException, RemoteException {
        return getProxy().getClusteredSpace(spaceName);
    }

    public String getName() throws RemoteException {
        if (_name == null)
            _name = getProxy().getName();
        return _name;
    }

    public IJSpace getSpace(String spaceName) throws NoSuchNameException,
            RemoteException {
        return getProxy().getSpace(spaceName);
    }

    public String[] getSpaceNames() throws RemoteException {
        return getProxy().getSpaceNames();
    }

    public SpaceURL getURL() throws RemoteException {
        return getProxy().getURL();
    }

    public void ping() throws RemoteException {
        getProxy().ping();
    }

    public static final ThreadLocal<Boolean> isEmbeddedShutdownInvocation = new ThreadLocal<Boolean>() {
        protected Boolean initialValue() {
            return false;
        }

        ;
    };

    public void shutdown() throws RemoteException {
        try {
            isEmbeddedShutdownInvocation.set(true);
            getProxy().shutdown();
        } catch (ConnectException e) {
            if (e.getCause() instanceof ClosedChannelException)
                return;
            throw e;
        } finally {
            isEmbeddedShutdownInvocation.set(false);
        }
    }

    public ContainerConfig getConfig() throws RemoteException {
        return ((IJSpaceContainerAdmin) getProxy()).getConfig();
    }

    public String getRuntimeConfigReport() throws RemoteException {
        return ((IJSpaceContainerAdmin) getProxy()).getRuntimeConfigReport();
    }

    public void setConfig(ContainerConfig config) throws RemoteException {
        ((IJSpaceContainerAdmin) getProxy()).setConfig(config);
    }
}
