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

package com.gigaspaces.internal.cluster.node.impl.router.spacefinder;

import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.lookup.entry.State;

import java.util.LinkedList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class SpaceUrlConverter
        implements ISpaceUrlConverter {

    private final List<String> _groupMembersNames;
    @SuppressWarnings("deprecation")
    private final List<SpaceURL> _groupMembersUrls;
    private final long _finderTimeout;

    public SpaceUrlConverter(ReplicationPolicy replicationPolicy) {
        _finderTimeout = replicationPolicy.m_SpaceFinderTimeout;
        _groupMembersNames = new LinkedList<String>(replicationPolicy.m_ReplicationGroupMembersNames);
        _groupMembersUrls = new LinkedList<SpaceURL>(replicationPolicy.m_ReplicationGroupMembersURLs);
    }

    @SuppressWarnings("deprecation")
    public SpaceURL convertMemberName(String memberLookupName) {
        SpaceURL result = convertReplicationGroupUrl(memberLookupName);
        if (result != null)
            return result;

        throw new IllegalArgumentException("Could not convert " + memberLookupName + " to SpaceURL");
    }

    private SpaceURL convertReplicationGroupUrl(String memberLookupName) {
        final int position = _groupMembersNames.indexOf(memberLookupName);
        if (position == -1)
            return null;

        SpaceURL spaceUrl = _groupMembersUrls.get(position).clone();
        if (JSpaceUtilities.isEmpty(spaceUrl.getProperty(SpaceURL.TIMEOUT)))
            spaceUrl.setProperty(SpaceURL.TIMEOUT, String.valueOf(_finderTimeout));
        spaceUrl.setLookupAttribute(SpaceURL.STATE, new State().setReplicable(Boolean.TRUE));

        return spaceUrl;
    }
}
