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

import com.gigaspaces.internal.query.QueryUtils;
import com.gigaspaces.management.space.SpaceQueryDetails;

import org.openspaces.pu.service.PlainServiceDetails;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A generic event container service details.
 *
 * @author kimchy
 */
public class EventContainerServiceDetails extends PlainServiceDetails {

    private static final long serialVersionUID = 4051111058959971069L;
    public static final String SERVICE_TYPE = "event-container";

    public static class Attributes {
        public static final String TEMPLATE = "template";
        public static final String QUERY_DETAILS = "query-details";
        public static final String PERFORM_SNAPSHOT = "perform-snapshot";
        public static final String GIGA_SPACE = "giga-space";
        public static final String TRANSACTION_MANAGER = "transaction-manager";
    }

    public EventContainerServiceDetails() {
        super();
    }

    public EventContainerServiceDetails(String id, String serviceSubType, String gigaSpace, String description, String longDescription,
                                        Object template, boolean performSnapshot, String transctionManager) {
        super(id, SERVICE_TYPE, serviceSubType, description, longDescription);
        getAttributes().put(Attributes.TEMPLATE, template);
        getAttributes().put(Attributes.QUERY_DETAILS, new SpaceQueryDetails(QueryUtils.getQueryDescription(template)));
        getAttributes().put(Attributes.GIGA_SPACE, gigaSpace);
        getAttributes().put(Attributes.PERFORM_SNAPSHOT, performSnapshot);
        getAttributes().put(Attributes.TRANSACTION_MANAGER, transctionManager);
    }

    public Object getTemplate() {
        return getAttributes().get(Attributes.TEMPLATE);
    }

    public SpaceQueryDetails getQueryDetails() {
        return (SpaceQueryDetails) getAttributes().get(Attributes.QUERY_DETAILS);
    }

    public Boolean isPerformSnapshot() {
        return (Boolean) getAttributes().get(Attributes.PERFORM_SNAPSHOT);
    }

    public String getGigaSpace() {
        return (String) getAttributes().get(Attributes.GIGA_SPACE);
    }

    public Boolean isTransactional() {
        return getAttributes().get(Attributes.TRANSACTION_MANAGER) != null;
    }

    public String getTransactionManager() {
        return (String) getAttributes().get(Attributes.TRANSACTION_MANAGER);
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
