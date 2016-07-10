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
package org.jini.rio.core.jsb;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * A Service state, defined by one of org.jini.rio.core.jsb.ServiceBeanState .
 *
 * @author moran
 * @since 6.6
 */
public interface ServiceState extends Remote {

    /**
     * returns one of the org.jini.rio.core.jsb.ServiceBeanState
     */
    int getState() throws RemoteException;

}
