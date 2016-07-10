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

package com.j_spaces.core.client;

import com.gigaspaces.internal.remoting.routing.clustered.LookupType;
import com.j_spaces.core.LookupManager;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.XPathProperties;

import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;

import java.util.Properties;
import java.util.StringTokenizer;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class LookupRequest {

    private final Class serviceClass;
    private String serviceName;
    private Entry[] serviceAttributes;
    private String locators;
    private String groups;
    private Properties customProperties;
    private long timeout;
    private long lookupInterval = LookupFinder.DEFAULT_INTERVAL_TIMEOUT;
    private LookupType lookupType = LookupType.TimeBasedLastIteration;

    public LookupRequest(Class serviceClass) {
        this.serviceClass = serviceClass;
    }

    public static LookupRequest TransactionManager() {
        return new LookupRequest(net.jini.core.transaction.server.TransactionManager.class);
    }

    public static LookupRequest IJSpace() {
        return new LookupRequest(com.j_spaces.core.IJSpace.class);
    }

    public String getServiceName() {
        return serviceName;
    }

    public LookupRequest setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public Class getServiceClass() {
        return serviceClass;
    }

    public Entry[] getServiceAttributes() {
        return serviceAttributes;
    }

    public LookupRequest setServiceAttributes(Entry[] attributes) {
        this.serviceAttributes = attributes;
        return this;
    }

    public String getLocators() {
        return locators;
    }

    public LookupRequest setLocators(String lookupHost) {
        this.locators = lookupHost;
        return this;
    }

    public String getGroups() {
        return groups;
    }

    public LookupRequest setGroups(String groups) {
        this.groups = groups;
        return this;
    }

    public String[] getLookupGroups() {
        final String PUBLIC = "public";
        final String PUBLIC_GROUP = "";
        final String[] DEFAULT_GROUPS = {PUBLIC_GROUP};

        if (groups == null || groups.equalsIgnoreCase(PUBLIC))
            return DEFAULT_GROUPS;
        StringTokenizer st = new StringTokenizer(groups, ",");
        String[] result = new String[st.countTokens()];

        int i = 0;
        while (st.hasMoreTokens()) {
            String gr = st.nextToken().trim();
            result[i] = gr.equalsIgnoreCase(PUBLIC) ? PUBLIC_GROUP : gr;
            i++;
        }

        return result;
    }

    public LookupLocator[] getLookupLocators() {
        //check if unicast lookups hosts list was passed using the xpath setting in the custom props
        //e.g.
        // com.j_spaces.core.container.directory_services.jini_lus.unicast_discovery.lus_host=localhost:4162
        if (customProperties != null) {
            //if XPATH was passed we use it as the final and merged Jini locators list
            String customLookupHost = customProperties.getProperty(XPathProperties.CONTAINER_JINI_LUS_UNICAST_HOSTS);
            if (!JSpaceUtilities.isEmpty(customLookupHost))
                locators = customLookupHost;
        }

        return locators != null ? LookupManager.buildLookupLocators(locators) : new LookupLocator[0];
    }

    public Properties getCustomProperties() {
        return customProperties;
    }

    public LookupRequest setCustomProperties(Properties customProperties) {
        this.customProperties = customProperties;
        return this;
    }

    public long getTimeout() {
        return timeout;
    }

    public LookupRequest setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public LookupType getLookupType() {
        return lookupType;
    }

    public LookupRequest setLookupType(LookupType lookupType) {
        this.lookupType = lookupType;
        return this;
    }

    public long getLookupInterval() {
        return lookupInterval;
    }

    public LookupRequest setLookupInterval(long lookupInterval) {
        this.lookupInterval = lookupInterval;
        return this;
    }
}
