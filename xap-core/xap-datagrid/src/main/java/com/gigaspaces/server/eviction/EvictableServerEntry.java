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


package com.gigaspaces.server.eviction;

import com.gigaspaces.server.ServerEntry;

/**
 * Encapsulates information about a server entry.
 *
 * @author Yechiel Feffer
 * @see SpaceEvictionStrategy
 * @since 9.1
 */
public interface EvictableServerEntry extends ServerEntry {
    /**
     * Returns the entry unique space identifier.
     */
    public String getUID();

    /**
     * in order to avoid searching the eviction handler data structures when an existing entry is
     * rendered to it by the space cache manager, the eviction-handler may store a payload to the
     * entry in it data structure and retrieve it later
     *
     * @return the eviction payload to the entry, or null if not stored
     */
    public Object getEvictionPayLoad();

    /**
     * in order to avoid searching the eviction handler data structures when an existing entry is
     * rendered to it by the space cache manager, the eviction-handler may store a payload to the
     * entry in it data structure and retrieve it later
     */
    void setEvictionPayLoad(Object evictionPayLoad);
}
