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


package com.j_spaces.core.admin;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @deprecated Since 8.0 - This interface is reserved for internal usage only.
 */
@Deprecated
public interface IJSpaceContainerAdmin extends Remote {
    /**
     * Returns name of Container Admin.
     *
     * @return String Container name
     * @throws RemoteException Failed to retrieve container name.
     **/
    public String getName() throws RemoteException;

    /**
     * Get container configuration.
     *
     * @return ContainerConfig The container config structure.
     * @throws RemoteException Failed to retrieve container configuration.
     **/
    public ContainerConfig getConfig() throws RemoteException;

    /**
     * Set new container configuration.
     *
     * @param config The container config structure.
     * @throws RemoteException Failed to set new container configuration.
     **/
    public void setConfig(ContainerConfig config) throws RemoteException;

    /**
     * Returns runtime configuration report: dump of the overall system configurations (spaces,
     * container, cluster), system env, sys properties.
     *
     * @return runtime configuration report
     * @throws RemoteException Failed to get runtime configuration report.
     */
    public String getRuntimeConfigReport() throws RemoteException;
}