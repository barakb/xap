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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.management.space.LocalCacheDetails;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class LocalCacheRegistrations {
    private final Map<String, LocalCacheDetails> _localCaches;

    public LocalCacheRegistrations() {
        this._localCaches = new ConcurrentHashMap<String, LocalCacheDetails>();
    }

    public Map<String, LocalCacheDetails> get() {
        return _localCaches;
    }

    public void add(NotifyTemplateHolder template) {
        ConnectionEndpointDetails connectionDetails = LRMIUtilities.getConnectionEndpointDetails(template.getREListener());

        LocalCacheDetails localCacheDetails = new LocalCacheDetails(template.getUID(), connectionDetails);
        _localCaches.put(localCacheDetails.getId(), localCacheDetails);
    }

    public void remove(NotifyTemplateHolder template) {
        _localCaches.remove(template.getUID());
    }
}
