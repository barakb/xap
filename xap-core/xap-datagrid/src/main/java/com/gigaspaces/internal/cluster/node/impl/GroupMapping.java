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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;

import java.util.Arrays;

/**
 * Represents mapping of {@link IReplicationSourceGroup}s that is used to determine to which groups
 * to dispatch outgoing replication
 *
 * @author eitany
 * @see ReplicationNode
 * @see ReplicationNodeConfig
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class GroupMapping {

    private final String[] _specificGroups;
    private final static GroupMapping _allMapping = new GroupMapping((String[]) null);

    /**
     * Create a group mapping representing specific named groups
     *
     * @param specificGroups mapped groups
     */
    public static GroupMapping createSpecificMapping(String... specificGroups) {
        return new GroupMapping(specificGroups);
    }

    /**
     * Gets a group mapping representing all existing groups
     */
    public static GroupMapping getAllMapping() {
        return _allMapping;
    }

    private GroupMapping(String... specificGroups) {
        _specificGroups = specificGroups;
    }

    /**
     * Gets the mapped groups
     *
     * @return null indicated all groups
     */
    public String[] getSpecificGroups() {
        return _specificGroups;
    }

    /**
     * Gets if this mapping has specific groups or it is representing all groups
     */
    public boolean hasSpecificGroups() {
        return _specificGroups != null;
    }

    @Override
    public String toString() {
        if (hasSpecificGroups())
            return Arrays.toString(_specificGroups);

        return "all";
    }
}
