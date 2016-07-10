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

package com.j_spaces.core.cluster;

import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.j_spaces.core.client.SpaceURL;

import java.io.Serializable;

/**
 * This class is maintained for backwards compatibility. NOTE: Since it is not externalizable, its
 * members cannot be changed.
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class LBProxyHolder implements Serializable {
    private static final long serialVersionUID = 50762093339202979L;

    /**
     * Load balance GroupMember state. Indicates the current state of the group member
     *
     * @author anna
     * @since 6.1
     */
    public static enum State {
        NOT_AVAILABLE, BACKUP, AVAILABLE
    }

    private final String _memberName;
    private final SpaceURL _memberUrl;
    private final IRemoteSpace _remoteSpace;
    public long timeStamp;
    private volatile State state = State.AVAILABLE;
    private final long _findTimeout;
    private volatile boolean _inUse;
    private volatile boolean _recentlyChecked;

    public LBProxyHolder(String memberName, SpaceURL spaceURL, IRemoteSpace remoteSpace) {
        _memberName = memberName;
        _memberUrl = spaceURL;
        _remoteSpace = remoteSpace;
        _findTimeout = Long.parseLong(spaceURL.getProperty(SpaceURL.TIMEOUT, "0"));
    }

    @Override
    public String toString() {
        return "Member: [" + _memberName + "] URL: [" + _memberUrl + "]";
    }

    public IRemoteSpace getRemoteSpace() {
        return _remoteSpace;
    }
}
