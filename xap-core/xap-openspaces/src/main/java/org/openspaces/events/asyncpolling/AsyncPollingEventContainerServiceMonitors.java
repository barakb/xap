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

import org.openspaces.events.EventContainerServiceMonitors;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Async Polling container service monitors.
 *
 * @author kimchy
 */
public class AsyncPollingEventContainerServiceMonitors extends EventContainerServiceMonitors {

    private static final long serialVersionUID = -6863158719393041020L;

    public static class Attributes extends EventContainerServiceMonitors.Attributes {
    }

    public AsyncPollingEventContainerServiceMonitors() {
        super();
    }

    public AsyncPollingEventContainerServiceMonitors(String id, long processedEvents, long failedEvents, String status) {
        super(id, processedEvents, failedEvents, status);
    }

    public AsyncPollingEventContainerServiceDetails getAsyncPollingDetails() {
        return (AsyncPollingEventContainerServiceDetails) getEventDetails();
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