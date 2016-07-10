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


package org.openspaces.events.support;

import com.gigaspaces.internal.dump.InternalDump;
import com.gigaspaces.internal.dump.InternalDumpProcessor;
import com.gigaspaces.internal.dump.InternalDumpProcessorFailedException;

import org.openspaces.events.AbstractEventListenerContainer;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitors;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.beans.factory.DisposableBean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds dynamically generated event containers.
 *
 * @author kimchy
 */
public class EventContainersBus implements DisposableBean, ServiceDetailsProvider, ServiceMonitorsProvider, InternalDumpProcessor {

    public static final String SUFFIX = "_eventContainer";

    private ConcurrentHashMap<String, AbstractEventListenerContainer> containers = new ConcurrentHashMap<String, AbstractEventListenerContainer>();

    public void registerContainer(String name, AbstractEventListenerContainer container) {
        containers.put(name + SUFFIX, container);
    }

    public boolean unregisterContainer(String name) {
        AbstractEventListenerContainer container = containers.remove(name + SUFFIX);
        if (container != null) {
            container.destroy();
            return true;
        }
        return false;
    }

    public AbstractEventListenerContainer getEventContainer(String name) {
        return containers.get(name + SUFFIX);
    }

    public Collection<AbstractEventListenerContainer> getEventContainers() {
        return new ArrayList<AbstractEventListenerContainer>(containers.values());
    }

    public ServiceDetails[] getServicesDetails() {
        ArrayList<ServiceDetails> list = new ArrayList<ServiceDetails>();
        for (AbstractEventListenerContainer container : containers.values()) {
            Collections.addAll(list, container.getServicesDetails());
        }
        return list.toArray(new ServiceDetails[list.size()]);
    }

    public ServiceMonitors[] getServicesMonitors() {
        ArrayList<ServiceMonitors> list = new ArrayList<ServiceMonitors>();
        for (AbstractEventListenerContainer container : containers.values()) {
            Collections.addAll(list, container.getServicesMonitors());
        }
        return list.toArray(new ServiceMonitors[list.size()]);
    }

    public String getName() {
        return "event-container-bus";
    }

    public void process(InternalDump dump) throws InternalDumpProcessorFailedException {
        for (AbstractEventListenerContainer container : containers.values()) {
            if (container instanceof InternalDumpProcessor) {
                ((InternalDumpProcessor) container).process(dump);
            }
        }
    }

    public void destroy() throws Exception {
        for (AbstractEventListenerContainer container : containers.values()) {
            container.destroy();
        }
        containers.clear();
    }
}
