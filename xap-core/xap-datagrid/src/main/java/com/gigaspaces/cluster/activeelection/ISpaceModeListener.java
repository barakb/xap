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


package com.gigaspaces.cluster.activeelection;

import java.rmi.RemoteException;

/**
 * Defines a callback listener to be notified when a space mode is changed.
 *
 * @author anna
 * @version 1.0
 * @see SpaceMode
 * @see SpaceComponentManager
 * @since 5.1
 */
public interface ISpaceModeListener extends java.rmi.Remote, java.util.EventListener {
    /**
     * Called right before the space mode changes.<br> Implementors usually use it to allow specific
     * service initializations.
     *
     * @param newMode the new mode change before it's published.
     * @throws RemoteException if an exception occurred during execution of a remote call.
     */
    void beforeSpaceModeChange(SpaceMode newMode) throws RemoteException;

    /**
     * Called right after the space mode changes. <br> Implementors usually use it to invoke any
     * needed services.
     *
     * @param newMode the new mode change after it was published.
     * @throws RemoteException if an exception occurred during execution of a remote call.
     */
    void afterSpaceModeChange(SpaceMode newMode) throws RemoteException;
}
