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
package net.jini.core.lookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges together a set of ServiceItems extracted from a ServiceMatches result. Provides a {@link
 * #getServiceMatches()} which returns an instance of a {@link ServiceMatches} which holds all the
 * merged ServiceItems from different {@link ServiceMatches}. The merged result will not contain
 * ServiceItem duplicates which match by their ServiceID.
 *
 * @author Moran Avigdor
 */
@com.gigaspaces.api.InternalApi
public class ServiceMatchesMerger {

    private final Map<ServiceID, ServiceItem> serviceItems = new HashMap<ServiceID, ServiceItem>();

    /**
     * Adds ServiceItems extracted from the ServiceMatches.
     */
    public void addServiceItems(ServiceMatches matches) {
        if (matches == null || matches.items == null) {
            return;
        }
        for (ServiceItem item : matches.items) {
            serviceItems.put(item.serviceID, item);
        }
    }

    /**
     * Returns an instance of {@link ServiceMatches} which contains all the added {@link
     * ServiceItem}s. The {@link ServiceMatches} returned will not contain duplicates (ServiceItems
     * with the same ServiceID).
     */
    public ServiceMatches getServiceMatches() {
        List<ServiceItem> merged = new ArrayList<ServiceItem>();
        for (ServiceItem item : serviceItems.values()) {
            merged.add(item);
        }
        ServiceItem[] mergedServiceItems = merged.toArray(new ServiceItem[merged.size()]);
        ServiceMatches mergedServiceMatches = new ServiceMatches(mergedServiceItems, mergedServiceItems.length);
        return mergedServiceMatches;
    }
}
