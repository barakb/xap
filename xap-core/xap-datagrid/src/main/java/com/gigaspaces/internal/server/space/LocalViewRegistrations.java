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

import com.gigaspaces.management.space.LocalViewDetails;
import com.gigaspaces.management.space.SpaceQueryDetails;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class LocalViewRegistrations {
    private final Map<String, LocalViewDetails> _localViews;

    public LocalViewRegistrations() {
        this._localViews = new ConcurrentHashMap<String, LocalViewDetails>();
    }

    public Map<String, LocalViewDetails> get() {
        return _localViews;
    }

    public void add(String localViewId, ConnectionEndpointDetails connectionDetails, Collection<SpaceQueryDetails> queries) {
        LocalViewDetails localViewDetails = new LocalViewDetails(localViewId, connectionDetails, queries);
        _localViews.put(localViewDetails.getId(), localViewDetails);
    }

    public void remove(String localViewId) {
        _localViews.remove(localViewId);
    }
}
