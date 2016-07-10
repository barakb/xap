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


package org.openspaces.pu.container.support;

import com.gigaspaces.internal.dump.InternalDumpProcessor;

import org.openspaces.admin.quiesce.QuiesceStateChangedListener;
import org.openspaces.core.cluster.MemberAliveIndicator;
import org.openspaces.core.cluster.ProcessingUnitUndeployingListener;
import org.openspaces.pu.container.CannotCloseContainerException;
import org.openspaces.pu.container.ProcessingUnitContainer;
import org.openspaces.pu.service.InvocableService;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitorsProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compound processing unit container wraps several processing unit containers and allows to close
 * them.
 *
 * @author kimchy
 */
public class CompoundProcessingUnitContainer extends ProcessingUnitContainer {

    private final ProcessingUnitContainer[] containers;

    public CompoundProcessingUnitContainer(ProcessingUnitContainer[] containers) {
        this.containers = containers;
    }

    /**
     * @return the underlying processing unit containers.
     * @since 8.0.3
     */
    public ProcessingUnitContainer[] getProcessingUnitContainers() {
        return containers;
    }

    @Override
    public void close() throws CannotCloseContainerException {
        for (ProcessingUnitContainer container : containers) {
            container.close();
        }
        super.close();
    }

    @Override
    public Collection<ServiceDetailsProvider> getServiceDetailsProviders() {
        List<ServiceDetailsProvider> result = Collections.EMPTY_LIST;
        for (ProcessingUnitContainer container : containers) {
            Collection<ServiceDetailsProvider> providers = container.getServiceDetailsProviders();
            if (!providers.isEmpty()) {
                if (result.isEmpty())
                    result = new ArrayList<ServiceDetailsProvider>();
                result.addAll(providers);
            }
        }
        return result;
    }

    @Override
    public Collection<ServiceMonitorsProvider> getServiceMonitorsProviders() {
        List<ServiceMonitorsProvider> result = Collections.EMPTY_LIST;
        for (ProcessingUnitContainer container : containers) {
            Collection<ServiceMonitorsProvider> providers = container.getServiceMonitorsProviders();
            if (!providers.isEmpty()) {
                if (result.isEmpty())
                    result = new ArrayList<ServiceMonitorsProvider>();
                result.addAll(providers);
            }
        }
        return result;
    }

    @Override
    public Collection<QuiesceStateChangedListener> getQuiesceStateChangedListeners() {
        List<QuiesceStateChangedListener> result = Collections.EMPTY_LIST;
        for (ProcessingUnitContainer container : containers) {
            Collection<QuiesceStateChangedListener> listeners = container.getQuiesceStateChangedListeners();
            if (!listeners.isEmpty()) {
                if (result.isEmpty())
                    result = new ArrayList<QuiesceStateChangedListener>();
                result.addAll(listeners);
            }
        }
        return result;
    }

    @Override
    public Collection<ProcessingUnitUndeployingListener> getUndeployListeners() {
        List<ProcessingUnitUndeployingListener> result = Collections.EMPTY_LIST;
        for (ProcessingUnitContainer container : containers) {
            Collection<ProcessingUnitUndeployingListener> listeners = container.getUndeployListeners();
            if (!listeners.isEmpty()) {
                if (result.isEmpty())
                    result = new ArrayList<ProcessingUnitUndeployingListener>();
                result.addAll(listeners);
            }
        }
        return result;
    }

    @Override
    public Collection<MemberAliveIndicator> getMemberAliveIndicators() {
        List<MemberAliveIndicator> result = Collections.EMPTY_LIST;
        for (ProcessingUnitContainer container : containers) {
            Collection<MemberAliveIndicator> indicators = container.getMemberAliveIndicators();
            if (!indicators.isEmpty()) {
                if (result.isEmpty())
                    result = new ArrayList<MemberAliveIndicator>();
                result.addAll(indicators);
            }
        }
        return result;
    }

    @Override
    public Collection<InternalDumpProcessor> getDumpProcessors() {
        List<InternalDumpProcessor> result = Collections.EMPTY_LIST;
        for (ProcessingUnitContainer container : containers) {
            Collection<InternalDumpProcessor> processors = container.getDumpProcessors();
            if (!processors.isEmpty()) {
                if (result.isEmpty())
                    result = new ArrayList<InternalDumpProcessor>();
                result.addAll(processors);
            }
        }
        return result;
    }

    @Override
    public Map<String, InvocableService> getInvocableServices() {
        Map<String, InvocableService> result = Collections.EMPTY_MAP;
        for (ProcessingUnitContainer container : containers) {
            Map<String, InvocableService> services = container.getInvocableServices();
            if (!services.isEmpty()) {
                if (result.isEmpty())
                    result = new HashMap<String, InvocableService>();
                result.putAll(services);
            }
        }
        return result;
    }
}
