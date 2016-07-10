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

package com.gigaspaces.internal.naming;

import com.j_spaces.core.client.LookupFinder;
import com.j_spaces.core.jini.SharedDiscoveryManagement;
import com.j_spaces.core.service.AbstractService;

import net.jini.admin.Administrable;
import net.jini.admin.JoinAdmin;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.lookup.ServiceItemFilter;

import java.rmi.RemoteException;

/**
 * The <code>LookupNamingService</code> class provides methods for obtaining references to remote
 * services in a Jini LookupService. Each method of the <code>LookupNamingService</code> takes as
 * one of its arguments a {@link net.jini.core.lookup.ServiceTemplate} to lookup the desired
 * service.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @since 6.0
 **/
@com.gigaspaces.api.InternalApi
public class LookupNamingService
        implements INamingService {
    private ServiceDiscoveryManager _serviceDiscoveryManager;

    public LookupNamingService(LookupDiscoveryManager discoveryManager, LeaseRenewalManager renewalManager)
            throws RemoteException {
        try {
            _serviceDiscoveryManager = SharedDiscoveryManagement.getBackwardsServiceDiscoveryManager(discoveryManager.getGroups(), discoveryManager.getLocators(), null);
        } catch (Exception ex) {
            throw new RemoteException("Failed to initialize LookupNamingService.", ex);
        }
    }


    /**
     * @see com.gigaspaces.internal.naming.INamingService#lookup(net.jini.core.lookup.ServiceTemplate,
     * int, net.jini.lookup.ServiceItemFilter)
     **/
    public ServiceItem[] lookup(ServiceTemplate srvTemplate, int maxMatches, ServiceItemFilter filter) {
        // sort before lookup so we will have a predictable order of templates
        LookupFinder.sortLookupAttributes(srvTemplate.attributeSetTemplates);
        ServiceItem[] serviceItems = _serviceDiscoveryManager.lookup(srvTemplate, maxMatches, filter);
        if (serviceItems.length == 0) {
            //check that there are any lookup services registered
            ServiceRegistrar[] registrars = _serviceDiscoveryManager.getDiscoveryManager().getRegistrars();
            if (registrars.length == 0) {
                return null; //indicate that there are no lookup services registered
            }
        }
        return serviceItems; //can be zero length if lookup services are registered and no matches for template
    }


    public Entry[] getLookupAttributes(Object service) throws RemoteException {
        return getJoinAdmin(service).getLookupAttributes();
    }


    public void modifyNamingAttributes(Object service, Entry[] attrSetTemplates, Entry[] attrSets)
            throws RemoteException {
        JoinAdmin joinAdmin = getJoinAdmin(service);

	  /* if this joinAdmin managed by GigaSpaces, allow to change the ServiceControlled entry  */
        if (joinAdmin instanceof AbstractService)
            ((AbstractService) joinAdmin).modifyLookupAttributes(attrSetTemplates, attrSets);
        else
            joinAdmin.modifyLookupAttributes(attrSetTemplates, attrSets);
    }


    /**
     * @see com.gigaspaces.internal.naming.INamingService#addNamingAttributes(Object,
     * net.jini.core.entry.Entry[])
     **/
    public void addNamingAttributes(Object service, Entry[] attrSets) throws RemoteException {
        JoinAdmin joinAdmin = getJoinAdmin(service);

	  /* if this joinAdmin managed by GigaSpaces, allow to change the ServiceControlled entry  */
        if (joinAdmin instanceof AbstractService)
            ((AbstractService) joinAdmin).addLookupAttributes(attrSets);
        else
            joinAdmin.addLookupAttributes(attrSets);
    }

    @Override
    public int getNumberOfRegistrars() {
        return _serviceDiscoveryManager.getDiscoveryManager().getRegistrars().length;
    }

    public String getName() {
        return "Jini Lookup Service";
    }


    /**
     * @see com.gigaspaces.internal.naming.INamingService#terminate()
     **/
    public void terminate() {
        _serviceDiscoveryManager.terminate();
    }


    /**
     * @return the instance of JoinAdmin
     */
    private JoinAdmin getJoinAdmin(Object service)
            throws RemoteException {
        JoinAdmin joinAdmin;

        if (service instanceof Administrable) {
            Object joinObj = ((Administrable) service).getAdmin();

            /** the admin object should implement JoinAdmin interface in order to modifyAttribute on LookupService */
            if (joinObj instanceof JoinAdmin) {
                joinAdmin = (JoinAdmin) joinObj;
            } else {
                throw new IllegalArgumentException("Failed to get JoinAdmin. Service: " + service.getClass().getName() + " must implement " + JoinAdmin.class.getName() + " interface.");
            }
        } else
            throw new IllegalArgumentException("Failed to get Administrable. Service: " + service.getClass().getName() + " must implement " + Administrable.class.getName() + " interface.");

        // can't be never null, otherwise exception
        return joinAdmin;
    }


    /**
     * @see com.gigaspaces.internal.naming.INamingService#notify(net.jini.core.lookup.ServiceTemplate,
     * net.jini.lookup.ServiceItemFilter, net.jini.lookup.ServiceDiscoveryListener)
     **/
    public LookupCache notify(ServiceTemplate tmpl, ServiceItemFilter filter, ServiceDiscoveryListener listener)
            throws RemoteException {
        // sort before lookup so we will have a predictable order of templates
        LookupFinder.sortLookupAttributes(tmpl.attributeSetTemplates);
        return _serviceDiscoveryManager.createLookupCache(tmpl, filter, listener);
    }

    public ServiceDiscoveryManager getServiceDiscoveryManager() {
        return _serviceDiscoveryManager;
    }
}