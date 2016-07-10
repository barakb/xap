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

package com.gigaspaces.internal.cluster.node.impl.processlog;

import com.gigaspaces.internal.cluster.node.impl.packets.HandleDataConsumeErrorPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationConnection;


@com.gigaspaces.api.InternalApi
public class DefaultProcessLogExceptionHandler
        implements IReplicationProcessLogExceptionHandler {

    private final IReplicationConnection _connectionToSource;
    private final String _groupName;

    public DefaultProcessLogExceptionHandler(
            IReplicationConnection sourceConnection, String sourceMemberLookupName, String groupName) {
        _groupName = groupName;
        _connectionToSource = sourceConnection;
    }

    public void close() {
        _connectionToSource.close();
    }

    public IDataConsumeFix handleException(IDataConsumeResult errorResult, IReplicationOrderedPacket packet)
            throws Exception {
        return _connectionToSource.dispatch(new HandleDataConsumeErrorPacket(errorResult, _groupName, packet != null ? packet.getKey() : -1));
    }

}
