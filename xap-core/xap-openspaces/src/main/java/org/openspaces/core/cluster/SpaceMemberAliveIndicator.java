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

package org.openspaces.core.cluster;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceHealthStatus;

import org.openspaces.core.util.SpaceUtils;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public class SpaceMemberAliveIndicator implements MemberAliveIndicator {
    private final boolean enabled;
    private final ISpaceProxy spaceProxy;

    public SpaceMemberAliveIndicator(IJSpace spaceProxy, Boolean enabled) {
        final boolean isEmbedded = !SpaceUtils.isRemoteProtocol(spaceProxy);
        this.enabled = enabled != null ? enabled : isEmbedded;
        this.spaceProxy = isEmbedded ? (ISpaceProxy) SpaceUtils.getClusterMemberSpace(spaceProxy) : (ISpaceProxy) spaceProxy;
    }

    @Override
    public boolean isMemberAliveEnabled() {
        return enabled;
    }

    @Override
    public boolean isAlive() throws Exception {
        //Check if the space is considered healthy
        SpaceHealthStatus spaceHealthStatus = spaceProxy.getSpaceHealthStatus();
        if (!spaceHealthStatus.isHealthy())
            throw spaceHealthStatus.getUnhealthyReason();
        return true;
    }
}
