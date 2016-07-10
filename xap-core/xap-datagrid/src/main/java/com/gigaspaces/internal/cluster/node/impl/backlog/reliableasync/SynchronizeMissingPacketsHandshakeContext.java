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

package com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync;

import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeContext;

/**
 * Used when the source replication backlog is missing packets that are in the target backlog, and
 * it requests from the target to complete its missing packets.
 *
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class SynchronizeMissingPacketsHandshakeContext
        implements IHandshakeContext {

    private final long _lastInsertedPacketToBacklog;
    private final long _lastProcessedKey;
    private boolean _done;

    public SynchronizeMissingPacketsHandshakeContext(
            long lastInsertedPacketToBacklog, long lastProcessedKey) {
        _lastInsertedPacketToBacklog = lastInsertedPacketToBacklog;
        _lastProcessedKey = lastProcessedKey;
    }

    @Override
    public boolean isDone() {
        return _done;
    }

    @Override
    public String toLogMessage() {
        return "Synchronize missing packets handshake context, needs to get missing packets from [" + _lastInsertedPacketToBacklog + "] to [" + _lastProcessedKey + "]";
    }

    public void setDone() {
        _done = true;
    }

}
