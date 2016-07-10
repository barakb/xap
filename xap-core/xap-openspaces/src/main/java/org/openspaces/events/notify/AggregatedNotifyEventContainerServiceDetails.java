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


package org.openspaces.events.notify;

import org.openspaces.events.EventContainerServiceDetails;
import org.openspaces.pu.service.PlainAggregatedServiceDetails;
import org.openspaces.pu.service.ServiceDetails;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Notify container service details.
 *
 * @author kimchy
 */
public class AggregatedNotifyEventContainerServiceDetails extends PlainAggregatedServiceDetails {

    private static final long serialVersionUID = -4365506459855928055L;

    public static class Attributes extends EventContainerServiceDetails.Attributes {
        public static final String COMM_TYPE = "comm-type";
        public static final String FIFO = "fifo";
        public static final String BATCH_SIZE = "batch-size";
        public static final String BATCH_TIME = "batch-time";
        public static final String AUTO_RENEW = "auto-renew";
        public static final String NOTIFY_WRITE = "notify-write";
        public static final String NOTIFY_TAKE = "notify-take";
        public static final String NOTIFY_UPDATE = "notify-update";
        public static final String NOTIFY_LEASE_EXPIRE = "notify-lease-expire";
        public static final String NOTIFY_UNMATCHED = "notify-unmatched";
        public static final String TRIGGER_NOTIFY_TEMPLATE = "trigger-notify-template";
        public static final String REPLICATE_NOTIFY_TEMPLATE = "replicate-notify-template";
        public static final String PERFORM_TAKE_ON_NOTIFY = "replicate-notify-template";
        public static final String PASS_ARRAY_AS_IS = "pass-array-as-is";
    }

    public AggregatedNotifyEventContainerServiceDetails() {
        super();
    }

    public AggregatedNotifyEventContainerServiceDetails(String serviceType, ServiceDetails[] details) {
        super(serviceType, details);
    }

    public String getCommType() {
        return (String) getAttributes().get(Attributes.COMM_TYPE);
    }

    public boolean isFifo() {
        return (Boolean) getAttributes().get(Attributes.FIFO);
    }

    public Integer getBatchSize() {
        return (Integer) getAttributes().get(Attributes.BATCH_SIZE);
    }

    public Integer getBatchTime() {
        return (Integer) getAttributes().get(Attributes.BATCH_TIME);
    }

    public boolean isAutoRenew() {
        return (Boolean) getAttributes().get(Attributes.AUTO_RENEW);
    }

    public boolean isNotifyWrite() {
        return (Boolean) getAttributes().get(Attributes.NOTIFY_WRITE);
    }

    public boolean isNotifyUpdate() {
        return (Boolean) getAttributes().get(Attributes.NOTIFY_UPDATE);
    }

    public boolean isNotifyTake() {
        return (Boolean) getAttributes().get(Attributes.NOTIFY_TAKE);
    }

    public boolean isNotifyLeaseExpire() {
        return (Boolean) getAttributes().get(Attributes.NOTIFY_LEASE_EXPIRE);
    }

    public boolean isNotifyUnmatched() {
        return (Boolean) getAttributes().get(Attributes.NOTIFY_UNMATCHED);
    }

    public Boolean isTriggerNotifyTemplate() {
        return (Boolean) getAttributes().get(Attributes.TRIGGER_NOTIFY_TEMPLATE);
    }

    public Boolean isReplicateNotifyTemplate() {
        return (Boolean) getAttributes().get(Attributes.REPLICATE_NOTIFY_TEMPLATE);
    }

    public boolean isPerformTakeOnNotify() {
        return (Boolean) getAttributes().get(Attributes.PERFORM_TAKE_ON_NOTIFY);
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