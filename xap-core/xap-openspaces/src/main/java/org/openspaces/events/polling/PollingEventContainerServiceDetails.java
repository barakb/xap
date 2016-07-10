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


package org.openspaces.events.polling;

import org.openspaces.events.EventContainerServiceDetails;
import org.openspaces.pu.service.AggregatedServiceDetails;
import org.openspaces.pu.service.ServiceDetails;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Polling container service details.
 *
 * @author kimchy
 */
public class PollingEventContainerServiceDetails extends EventContainerServiceDetails {

    private static final long serialVersionUID = 6240507580002461248L;

    public static final String SERVICE_SUB_TYPE = "polling";

    public static class Attributes extends EventContainerServiceDetails.Attributes {
        public static final String RECEIVE_TIMEOUT = "receive-timeout";
        public static final String RECEIVE_OPERATION_HANDLER = "receive-operating-handler";
        public static final String TRIGGER_OPERATION_HANDLER = "trigger-operating-handler";
        public static final String CONCURRENT_CONSUMERS = "concurrent-consumers";
        public static final String MAX_CONCURRENT_CONSUMERS = "max-concurrent-consumers";
        public static final String PASS_ARRAY_AS_IS = "pass-array-as-is";
        public static final String DYNAMIC_TEMPLATE = "dynamic-template";
    }

    public PollingEventContainerServiceDetails() {
        super();
    }

    public PollingEventContainerServiceDetails(String id, String gigaSpace, Object template, boolean performSnapshot, String transactionManager,
                                               long receiveTimeout,
                                               String receiveOperationHandler, String triggerOperationHandler,
                                               int concurrentConsumers, int maxConcurrentConsumers, boolean passArrayAsIs, boolean dynamicTemplate) {
        super(id, SERVICE_SUB_TYPE, gigaSpace, "Polling event container", "Polling event container, template [" + template + "]", template, performSnapshot, transactionManager);
        getAttributes().put(Attributes.RECEIVE_TIMEOUT, receiveTimeout);
        getAttributes().put(Attributes.RECEIVE_OPERATION_HANDLER, receiveOperationHandler);
        getAttributes().put(Attributes.TRIGGER_OPERATION_HANDLER, triggerOperationHandler);
        getAttributes().put(Attributes.CONCURRENT_CONSUMERS, concurrentConsumers);
        getAttributes().put(Attributes.MAX_CONCURRENT_CONSUMERS, maxConcurrentConsumers);
        getAttributes().put(Attributes.PASS_ARRAY_AS_IS, passArrayAsIs);
        getAttributes().put(Attributes.DYNAMIC_TEMPLATE, dynamicTemplate);
    }

    public Long getReceiveTimeout() {
        return (Long) getAttributes().get(Attributes.RECEIVE_TIMEOUT);
    }

    public String getReceiveOperationHandler() {
        return (String) getAttributes().get(Attributes.RECEIVE_OPERATION_HANDLER);
    }

    public String getTriggerOperationHandler() {
        return (String) getAttributes().get(Attributes.TRIGGER_OPERATION_HANDLER);
    }

    public Integer getConcurrentConsumers() {
        return (Integer) getAttributes().get(Attributes.CONCURRENT_CONSUMERS);
    }

    public Integer getMaxConcurrentConsumers() {
        return (Integer) getAttributes().get(Attributes.MAX_CONCURRENT_CONSUMERS);
    }

    public Boolean isPassArrayAsIs() {
        return (Boolean) getAttributes().get(Attributes.PASS_ARRAY_AS_IS);
    }

    public Boolean isDynamicTemplate() {
        return (Boolean) getAttributes().get(Attributes.DYNAMIC_TEMPLATE);
    }

    @Override
    public AggregatedServiceDetails aggregateByServiceSubType(ServiceDetails[] servicesDetails) {
        return new AggregatedPollingEventContainerServiceDetails(serviceType, servicesDetails);
    }

    @Override
    public AggregatedServiceDetails aggregateById(ServiceDetails[] servicesDetails) {
        return new AggregatedPollingEventContainerServiceDetails(serviceType, servicesDetails);
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
