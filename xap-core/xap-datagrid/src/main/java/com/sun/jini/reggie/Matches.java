/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.reggie;

import net.jini.core.lookup.ServiceMatches;
import net.jini.core.lookup.ServiceMatchesWrapper;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * A Matches contains the fields of a ServiceMatches packaged up for transmission between
 * client-side proxies and the registrar server. Instances are never visible to clients, they are
 * private to the communication between the proxies and the server. <p> This class only has a bare
 * minimum of methods, to minimize the amount of code downloaded into clients.
 *
 * @author Sun Microsystems, Inc.
 */
@com.gigaspaces.api.InternalApi
public class Matches implements Serializable, ServiceMatchesWrapper {

    private static final long serialVersionUID = 2L;

    /**
     * ServiceMatches.items as an ArrayList of Item
     *
     * @serial
     */
    public ArrayList items;
    /**
     * ServiceMatches.totalMatches
     *
     * @serial
     */
    public int totalMatches;

    /**
     * Simple constructor.
     */
    public Matches(ArrayList items, int totalMatches) {
        this.items = items;
        this.totalMatches = totalMatches;
    }

    /**
     * Converts a Matches to a ServiceMatches.
     */
    public ServiceMatches get() throws RemoteException {
        return new ServiceMatches(Item.toServiceItem(items), totalMatches);
    }
}
