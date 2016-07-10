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

@com.gigaspaces.api.InternalApi
public class ReliableAsyncHandshakeContext
        implements IHandshakeContext {

    private volatile long _minimumUnsentKey;
    private final long _lastProcessedKey;

    public ReliableAsyncHandshakeContext(long minimumUnconfirmedKey, long lastProcessedKey) {
        _minimumUnsentKey = minimumUnconfirmedKey;
        _lastProcessedKey = lastProcessedKey;
    }

    public boolean isDone() {
        return _minimumUnsentKey > _lastProcessedKey;
    }

    public long getMinimumUnsentKey() {
        return _minimumUnsentKey;
    }

    public void setMinimumUnsentKey(long minimumUnsentKey) {
        _minimumUnsentKey = minimumUnsentKey;
    }

    public long getLastProcessedKey() {
        return _lastProcessedKey;
    }

    public String toLogMessage() {
        return "Reliable async handshake context. Minimum unsent key [" + _minimumUnsentKey + "]. Last processed key by target [" + _lastProcessedKey + "].";
    }

}
