/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2005 GigaSpaces, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jini.rio.watch;


import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * This interface is used to fetch WatchDataSource instances that have been created and registered
 */
public interface Watchable extends Remote {
    /**
     * Returns an array of all WatchDataSource objects which provide a reference to an
     * implementation of WatchDataSource.
     */
    public WatchDataSource[] fetch() throws RemoteException;

    /**
     * Returns an array of WatchDataSource objects which match the input <pre>id</pre>
     * which corresponds to a <pre>Watch</pre> identifier. The WatchDataSource object(s)
     * returned provide a reference to an implementation of WatchDataSource.
     */
    public WatchDataSource[] fetch(String id) throws RemoteException;

}
