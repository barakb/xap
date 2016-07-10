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

package com.j_spaces.core.service;

import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.id.ReferentUuid;
import net.jini.id.ReferentUuids;
import net.jini.id.Uuid;

import java.io.Serializable;
import java.rmi.RemoteException;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class ServiceAdminProxy
        //extends AbstractProxy
        implements ServiceAdmin, Serializable, ReferentUuid {
    private static final long serialVersionUID = 3258412833011281977L;

    protected ServiceAdmin adminImpl;
    protected Uuid uuid;
    private int hashCode;

    /**
     * Returns AdminProxy or ConstrainableAdminProxy instance, depending on whether given server
     * implements RemoteMethodControl.
     */
    public static ServiceAdminProxy getInstance(ServiceAdmin adminImpl, Uuid uuid) {
        return new ServiceAdminProxy(adminImpl, uuid);
        /*return (admin instanceof RemoteMethodControl) ?
            new ConstrainableAdminProxy(admin, serviceID, null) :
		    new AdminProxy(admin, serviceID);*/
    }

    /**
     * Constructor for use by getInstance(), ConstrainableAdminProxy.
     */
    public ServiceAdminProxy(ServiceAdmin adminImpl, Uuid uuid) {
        //super(admin, uuid);
        this.adminImpl = adminImpl;
        this.uuid = uuid;
        this.hashCode = uuid.hashCode();
    }

    /**
     * Returns service ID hash code.
     */
    public int hashCode() {
        return this.hashCode;
    }

    /**
     * Proxies for servers with the same service ID are considered equal.
     */
    public boolean equals(Object obj) {
        return ReferentUuids.compare(this, obj);
    }

    // This method's javadoc is inherited from an interface of this class
    public void destroy() throws RemoteException {
        adminImpl.destroy();
    }

    // This method's javadoc is inherited from an interface of this class
    public Entry[] getLookupAttributes() throws RemoteException {
        return adminImpl.getLookupAttributes();
    }

    // This method's javadoc is inherited from an interface of this class
    public void addLookupAttributes(Entry[] attrSets) throws RemoteException {
        adminImpl.addLookupAttributes(attrSets);
    }

    // This method's javadoc is inherited from an interface of this class
    public void modifyLookupAttributes(Entry[] attrSetTemplates, Entry[] attrSets)
            throws RemoteException {
        adminImpl.modifyLookupAttributes(attrSetTemplates, attrSets);
    }

    // This method's javadoc is inherited from an interface of this class
    public String[] getLookupGroups() throws RemoteException {
        return adminImpl.getLookupGroups();
    }

    // This method's javadoc is inherited from an interface of this class
    public void addLookupGroups(String[] groups) throws RemoteException {
        adminImpl.addLookupGroups(groups);
    }

    // This method's javadoc is inherited from an interface of this class
    public void removeLookupGroups(String[] groups) throws RemoteException {
        adminImpl.removeLookupGroups(groups);
    }

    // This method's javadoc is inherited from an interface of this class
    public void setLookupGroups(String[] groups) throws RemoteException {
        adminImpl.setLookupGroups(groups);
    }

    // This method's javadoc is inherited from an interface of this class
    public LookupLocator[] getLookupLocators() throws RemoteException {
        return adminImpl.getLookupLocators();
    }

    // This method's javadoc is inherited from an interface of this class
    public void addLookupLocators(LookupLocator[] locators)
            throws RemoteException {
        adminImpl.addLookupLocators(locators);
    }

    // This method's javadoc is inherited from an interface of this class
    public void removeLookupLocators(LookupLocator[] locators)
            throws RemoteException {
        adminImpl.removeLookupLocators(locators);
    }

    // This method's javadoc is inherited from an interface of this class
    public void setLookupLocators(LookupLocator[] locators)
            throws RemoteException {
        adminImpl.setLookupLocators(locators);
    }

    // This method's javadoc is inherited from an interface of this class
    public Uuid getReferentUuid() {
        return uuid;
    }

}
