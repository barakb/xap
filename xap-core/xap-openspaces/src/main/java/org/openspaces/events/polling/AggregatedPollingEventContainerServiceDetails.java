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

import org.openspaces.pu.service.PlainAggregatedServiceDetails;
import org.openspaces.pu.service.ServiceDetails;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy
 */
public class AggregatedPollingEventContainerServiceDetails extends PlainAggregatedServiceDetails {

    private static final long serialVersionUID = -4531279106221740074L;

    public static class Attributes {
        public static final String RECEIVE_TIMEOUT = "receive-timeout";
        public static final String RECEIVE_OPERATION_HANDLER = "receive-operating-handler";
        public static final String TRIGGER_OPERATION_HANDLER = "trigger-operating-handler";
        public static final String CONCURRENT_CONSUMERS = "concurrent-consumers";
        public static final String MAX_CONCURRENT_CONSUMERS = "max-concurrent-consumers";
        public static final String PASS_ARRAY_AS_IS = "pass-array-as-is";
        public static final String TEMPLATE = "template";
    }

    public AggregatedPollingEventContainerServiceDetails() {
        super();
    }

    public AggregatedPollingEventContainerServiceDetails(String serviceType, ServiceDetails[] details) {
        super(serviceType, details);
        int concurrentConsumers = 0;
        int maxConcurrentConsumers = 0;
        for (ServiceDetails detail : details) {
            if (!(detail instanceof PollingEventContainerServiceDetails)) {
                throw new IllegalArgumentException("Details [" + detail.getClass().getName() + "] is of wrong type");
            }
            PollingEventContainerServiceDetails pollingServiceDetails = (PollingEventContainerServiceDetails) detail;
            concurrentConsumers += pollingServiceDetails.getConcurrentConsumers();
            maxConcurrentConsumers += pollingServiceDetails.getMaxConcurrentConsumers();
        }
        getAttributes().put(Attributes.CONCURRENT_CONSUMERS, concurrentConsumers);
        getAttributes().put(Attributes.MAX_CONCURRENT_CONSUMERS, maxConcurrentConsumers);
    }

    public long getReceiveTimeout() {
        return (Long) getAttributes().get(Attributes.RECEIVE_TIMEOUT);
    }

    public String getReceiveOperationHandler() {
        return (String) getAttributes().get(Attributes.RECEIVE_OPERATION_HANDLER);
    }

    public String getTriggerOperationHandler() {
        return (String) getAttributes().get(Attributes.TRIGGER_OPERATION_HANDLER);
    }

    public int getConcurrentConsumers() {
        return (Integer) getAttributes().get(Attributes.CONCURRENT_CONSUMERS);
    }

    public int getMaxConcurrentConsumers() {
        return (Integer) getAttributes().get(Attributes.MAX_CONCURRENT_CONSUMERS);
    }

    public boolean isPassArrayAsIs() {
        return (Boolean) getAttributes().get(Attributes.PASS_ARRAY_AS_IS);
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
