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


package org.openspaces.pu.container.spi;

import com.gigaspaces.internal.dump.InternalDumpProcessor;

import org.openspaces.admin.quiesce.QuiesceStateChangedListener;
import org.openspaces.core.cluster.MemberAliveIndicator;
import org.openspaces.core.cluster.ProcessingUnitUndeployingListener;
import org.openspaces.pu.container.ProcessingUnitContainer;
import org.openspaces.pu.service.InvocableService;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.context.ApplicationContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * A processing unit container that is based on Spring {@link ApplicationContext}.
 *
 * @author kimchy
 */
public abstract class ApplicationContextProcessingUnitContainer extends ProcessingUnitContainer {

    public abstract ApplicationContext getApplicationContext();

    @Override
    public Collection<ServiceDetailsProvider> getServiceDetailsProviders() {
        return getBeansOfType(ServiceDetailsProvider.class).values();
    }

    @Override
    public Collection<ServiceMonitorsProvider> getServiceMonitorsProviders() {
        return getBeansOfType(ServiceMonitorsProvider.class).values();
    }

    @Override
    public Collection<QuiesceStateChangedListener> getQuiesceStateChangedListeners() {
        return getBeansOfType(QuiesceStateChangedListener.class).values();
    }

    @Override
    public Collection<ProcessingUnitUndeployingListener> getUndeployListeners() {
        return getBeansOfType(ProcessingUnitUndeployingListener.class).values();
    }

    @Override
    public Collection<MemberAliveIndicator> getMemberAliveIndicators() {
        return getBeansOfType(MemberAliveIndicator.class).values();
    }

    @Override
    public Collection<InternalDumpProcessor> getDumpProcessors() {
        return getBeansOfType(InternalDumpProcessor.class).values();
    }

    @Override
    public Map<String, InvocableService> getInvocableServices() {
        return getBeansOfType(InvocableService.class);
    }

    private <T> Map<String, T> getBeansOfType(Class<T> type) {
        ApplicationContext applicationContext = getApplicationContext();
        return applicationContext != null
                ? applicationContext.getBeansOfType(type)
                : Collections.EMPTY_MAP;
    }
}
