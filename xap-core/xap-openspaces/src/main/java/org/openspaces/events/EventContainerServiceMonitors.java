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


package org.openspaces.events;

import org.openspaces.pu.service.PlainServiceMonitors;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A generic event container service monitors.
 *
 * @author kimchy
 */
public class EventContainerServiceMonitors extends PlainServiceMonitors {

    private static final long serialVersionUID = -6852327853548539168L;

    public static class Attributes {
        public static final String PROCESSED_EVENTS = "processed-events";
        public static final String FAILED_EVENTS = "failed-events";
        public static final String STATUS = "status";
    }

    public EventContainerServiceMonitors() {
        super();
    }

    public EventContainerServiceMonitors(String id, long processedEvents, long failedEvents, String status) {
        super(id);
        getMonitors().put(Attributes.PROCESSED_EVENTS, processedEvents);
        getMonitors().put(Attributes.FAILED_EVENTS, failedEvents);
        getMonitors().put(Attributes.STATUS, status);
    }

    public EventContainerServiceDetails getEventDetails() {
        return (EventContainerServiceDetails) getDetails();
    }

    public long getProcessedEvents() {
        return (Long) getMonitors().get(Attributes.PROCESSED_EVENTS);
    }

    public long getFailedEvents() {
        return (Long) getMonitors().get(Attributes.FAILED_EVENTS);
    }

    public String getStatus() {
        return (String) getMonitors().get(Attributes.STATUS);
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