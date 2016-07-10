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

import com.gigaspaces.config.lrmi.ITransportConfig;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ExportException;

/**
 * A filter that contains functions to control the life cycle of a space related stub.  It can be
 * used to implement SSL security in (some of the) space stubs or create a stub that implements a
 * different protocol from the com.j_spaces.core.StubHandlerImpl which is the default GigaSpaces
 * implementation (non-secure RMI)
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @deprecated Since 8.0 - This class is reserved for internal usage only.
 */
@Deprecated
public interface IStubHandler extends java.io.Serializable {
    /**
     * Exports the remote object to make it available to receive incoming calls.
     *
     * @param obj the remote object to be exported
     * @return remote object stub
     * @throws RemoteException if export fails
     */
    Remote exportObject(Remote obj) throws ExportException;


    /**
     * Removes the remote object, obj, from the StubHandler runtime. If successful, the object can
     * no longer accept incoming RMI calls.
     *
     * @param obj the object to disable stub for
     **/
    void unexportObject(Remote obj);

    /**
     * Returns the default transport config associated with this stub handler
     *
     * @return default transport config associated with this stub handler
     */
    ITransportConfig getTransportConfig();
}