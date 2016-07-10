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


package org.openspaces.events.asyncpolling;

import org.openspaces.events.EventContainerServiceDetails;
import org.openspaces.pu.service.AggregatedServiceDetails;
import org.openspaces.pu.service.ServiceDetails;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Async Polling container service details.
 *
 * @author kimchy
 */
public class AsyncPollingEventContainerServiceDetails extends EventContainerServiceDetails {

    private static final long serialVersionUID = -2888577681327987480L;

    public static final String SERVICE_SUB_TYPE = "async-polling";

    public static class Attributes extends EventContainerServiceDetails.Attributes {
        public static final String RECEIVE_TIMEOUT = "receive-timeout";
        public static final String CONCURRENT_CONSUMERS = "concurrent-consumers";
    }

    public AsyncPollingEventContainerServiceDetails() {
        super();
    }

    public AsyncPollingEventContainerServiceDetails(String id, String gigaSpace, Object template, boolean performSnapshot, String transactionManager,
                                                    long receiveTimeout, int concurrentConsumers) {
        super(id, SERVICE_SUB_TYPE, gigaSpace, "Async Polling event container", "Async Polling event container, template [" + template + "]", template, performSnapshot, transactionManager);
        getAttributes().put(Attributes.RECEIVE_TIMEOUT, receiveTimeout);
        getAttributes().put(Attributes.CONCURRENT_CONSUMERS, concurrentConsumers);
    }

    public Long getReceiveTimeout() {
        return (Long) getAttributes().get(Attributes.RECEIVE_TIMEOUT);
    }

    public Integer getConcurrentConsumers() {
        return (Integer) getAttributes().get(Attributes.CONCURRENT_CONSUMERS);
    }

    @Override
    public AggregatedServiceDetails aggregateByServiceSubType(ServiceDetails[] servicesDetails) {
        return new AggregatedAsyncPollingEventContainerServiceDetails(serviceType, servicesDetails);
    }

    @Override
    public AggregatedServiceDetails aggregateById(ServiceDetails[] servicesDetails) {
        return new AggregatedAsyncPollingEventContainerServiceDetails(serviceType, servicesDetails);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }
}