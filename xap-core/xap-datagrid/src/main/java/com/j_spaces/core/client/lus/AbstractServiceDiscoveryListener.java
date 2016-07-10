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

package com.j_spaces.core.client.lus;

import com.j_spaces.core.IJSpace;
import com.j_spaces.lookup.entry.ContainerName;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.entry.Name;

/**
 * A base implementation of {@link ServiceDiscoveryListener} in order to listen to {@link
 * net.jini.lookup.LookupCache}. This class should be extend inorder to listen to spaces
 * join/change/leave.
 *
 * It filters the incoming events according to the given parameters in the constructor.
 *
 * @author Guy Korland
 * @version 0.1
 * @since 5.1
 */
abstract public class AbstractServiceDiscoveryListener
        implements ServiceDiscoveryListener {
    final static private Entry[] EMPTY_ATTRIBUTES = new Entry[0];
    final static private Class[] EMPTY_CLASS = new Class[0];

    final private Name _serviceName;
    final private Class[] _classes;
    final private Entry[] _serviceAttributes;

    /**
     * Creates a new listener for the given service.
     *
     * @param serviceName name of the service
     */
    public AbstractServiceDiscoveryListener(String serviceName, Class[] classes,
                                            Entry[] serviceAttributes) {
        _serviceName = new Name(serviceName);
        _classes = classes == null ? EMPTY_CLASS : classes;
        _serviceAttributes = serviceAttributes == null ? EMPTY_ATTRIBUTES : serviceAttributes;
    }

    /**
     * {@inheritDoc} Call {@link #serviceAdded(String, IJSpace, ServiceID)} if the event is
     * relevant.
     */
    public void serviceAdded(ServiceDiscoveryEvent event) {
        ServiceItem item = event.getPostEventServiceItem();

        //if( isRelevant( item.service, item.attributeSets))
        // TODO check isRelevant
        Object service = item.getService();
        if (service instanceof IJSpace) {
            String fullName = buildFullName(item.attributeSets);
            serviceAdded(fullName, (IJSpace) service, item.serviceID);
        }
    }

    /**
     * {@inheritDoc} Call {@link #serviceChanged(String, IJSpace, ServiceID)} if the event is
     * relevant.
     */
    public void serviceChanged(ServiceDiscoveryEvent event) {
        ServiceItem postItem = event.getPostEventServiceItem();
        ServiceItem preItem = event.getPreEventServiceItem();

        Object postItemService = postItem.getService();
        Object preItemService = preItem.getService();
        if (isRelevant(postItemService, postItem.attributeSets) ||
                isRelevant(preItemService, preItem.attributeSets)) {
            String fullName = buildFullName(postItem.attributeSets);
            serviceChanged(fullName, (IJSpace) postItemService, postItem.serviceID);
        }
    }

    /**
     * {@inheritDoc} Call {@link #serviceRemoved(String, IJSpace, ServiceID)} if the event is
     * relevant.
     */
    public void serviceRemoved(ServiceDiscoveryEvent event) {
        ServiceItem item = event.getPreEventServiceItem();

        Object service = item.getService();
        if (isRelevant(service, item.attributeSets)) {
            String fullName = buildFullName(item.attributeSets);
            serviceRemoved(fullName, (IJSpace) service, item.serviceID);
        }
    }

    private String buildFullName(Entry[] attributeSets) {
        StringBuilder fullName = new StringBuilder();
        for (Entry entry : attributeSets) {
            if (entry instanceof ContainerName) {
                fullName.append(((ContainerName) entry).name);
                fullName.append(':');
            }
        }

        for (Entry entry : attributeSets) {
            if (entry instanceof Name) {
                fullName.append(((Name) entry).name);
            }
        }
        return fullName.toString();
    }

    /**
     * Is relevant service. Used as filter for the incoming events.
     *
     * @param service    service to check
     * @param attributes the service attributes
     * @return <code>true</code> if the given _serviceAttributes are all founded in the given
     * attributes.
     */
    private boolean isRelevant(Object service, Entry[] attributes) {
        // checks attributes
        LOOP:
        for (Entry e : _serviceAttributes) {
            for (Entry en : attributes) {
                if (e.equals(en))
                    continue LOOP;
            }
            return false; // not equal
        }

        // checks service name
        boolean flag = false;
        for (Entry en : attributes) {
            if (_serviceName.equals(en)) {
                flag = true;
                break;
            }
        }
        if (!flag)
            return false;

        // checks service name
        flag = false;
        for (Class cl : _classes) {
            if (cl.isInstance(service)) {
                flag = true;
                break;
            }
        }

        return flag;
    }

    /**
     * This method is called if the event is relevant according to the parameters provided in the
     * constructor.
     *
     * @param fullName space full name, host:container:space
     * @param space    source of the event.
     */
    abstract public void serviceAdded(String fullName, IJSpace space, ServiceID id);

    /**
     * This method is called if the event is relevant according to the parameters provided in the
     * constructor.
     *
     * @param fullName space full name, host:container:space
     * @param space    source of the event.
     */
    abstract public void serviceChanged(String fullName, IJSpace space, ServiceID id);

    /**
     * This method is called if the event is relevant according to the parameters provided in the
     * constructor.
     *
     * @param fullName space full name, host:container:space
     * @param space    source of the event.
     */
    abstract public void serviceRemoved(String fullName, IJSpace space, ServiceID id);
}
