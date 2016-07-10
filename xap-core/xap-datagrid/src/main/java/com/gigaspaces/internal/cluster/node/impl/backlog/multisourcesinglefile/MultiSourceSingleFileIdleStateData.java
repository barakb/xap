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

package com.gigaspaces.internal.cluster.node.impl.backlog.multisourcesinglefile;

import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author idan
 * @author eitan
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class MultiSourceSingleFileIdleStateData
        implements IIdleStateData {
    private static final long serialVersionUID = 1L;
    private long _lastConfirmedKey;
    private long _lastReceivedKey;

    public MultiSourceSingleFileIdleStateData() {
    }

    public MultiSourceSingleFileIdleStateData(long lastConfirmedKey,
                                              long lastReceivedKey) {
        _lastConfirmedKey = lastConfirmedKey;
        _lastReceivedKey = lastReceivedKey;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_lastConfirmedKey);
        out.writeLong(_lastReceivedKey);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _lastConfirmedKey = in.readLong();
        _lastReceivedKey = in.readLong();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    public long getLastConfirmedKey() {
        return _lastConfirmedKey;
    }

    public long getLastReceivedKey() {
        return _lastReceivedKey;
    }

    @Override
    public String toString() {
        return "Idle state data lastConfirmedKey [" + _lastConfirmedKey + "] lastReceivedKey [" + _lastReceivedKey + "]";
    }

    @Override
    public long size() {
        return _lastReceivedKey - _lastConfirmedKey;
    }
}
