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

package com.gigaspaces.internal.cluster.node.impl.packets.data.filters;


import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.cluster.ReplicationOperationType;

import java.rmi.MarshalledObject;

@com.gigaspaces.api.InternalApi
public class NotifyReplicationFilterUidDataWrapper
        extends ReplicationFilterUidDataWrapper {
    private static final long serialVersionUID = 1L;
    private final NotifyInfo _notifyInfo;

    public NotifyReplicationFilterUidDataWrapper(
            AbstractReplicationPacketSingleEntryData data, String uid,
            ITypeDesc typeDesc, ReplicationOperationType operationType,
            int objectType, NotifyInfo notifyInfo, ITimeToLiveUpdateCallback timeToLiveUpdateCallback) {
        super(data, uid, typeDesc, operationType, objectType, timeToLiveUpdateCallback);
        _notifyInfo = notifyInfo;
    }

    @Override
    public int getNotifyType() {
        return (_notifyInfo != null ? _notifyInfo.getNotifyType() : 0);
    }

    @Override
    public MarshalledObject getHandback() {
        if (_notifyInfo != null)
            return _notifyInfo.getHandback();
        else
            return null;
    }


}
