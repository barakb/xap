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

import org.openspaces.events.EventContainerServiceMonitors;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Polling container service monitors.
 *
 * @author kimchy
 */
public class PollingEventContainerServiceMonitors extends EventContainerServiceMonitors {

    private static final long serialVersionUID = 545174360485562728L;

    public static class Attributes extends EventContainerServiceMonitors.Attributes {
        public static final String CONSUMERS = "consumers";
    }

    public PollingEventContainerServiceMonitors() {
        super();
    }

    public PollingEventContainerServiceMonitors(String id, long processedEvents, long failedEvents, String status, int consumers) {
        super(id, processedEvents, failedEvents, status);
        getMonitors().put(Attributes.CONSUMERS, consumers);
    }

    public PollingEventContainerServiceDetails getPollingEventDetails() {
        return (PollingEventContainerServiceDetails) getEventDetails();
    }

    public int getConsumers() {
        return (Integer) getMonitors().get(Attributes.CONSUMERS);
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