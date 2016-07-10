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

package com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.AbstractMultiBucketSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileHandshakeResponse
        implements IProcessLogHandshakeResponse {
    private static final long serialVersionUID = 1L;
    private long[] _lastProcessesKeysBuckets;
    private long _lastProcessedGlobalKey;

    public MultiBucketSingleFileHandshakeResponse() {
    }

    public MultiBucketSingleFileHandshakeResponse(
            long[] lastProcessesKeysBuckets, long lastProcessedGlobalKey) {
        _lastProcessesKeysBuckets = lastProcessesKeysBuckets;
        _lastProcessedGlobalKey = lastProcessedGlobalKey;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_lastProcessedGlobalKey);
        IOUtils.writeLongArray(out, _lastProcessesKeysBuckets);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _lastProcessedGlobalKey = in.readLong();
        _lastProcessesKeysBuckets = IOUtils.readLongArray(in);
    }

    public long[] getLastProcessesKeysBuckets() {
        return _lastProcessesKeysBuckets;
    }

    public long getLastProcessedGlobalKey() {
        return _lastProcessedGlobalKey;
    }

    public String toLogMessage() {
        return "Handshake was successful. Last global process key by target is [" + getLastProcessedGlobalKey() + "] Last processed bucket keys by the target are " + AbstractMultiBucketSingleFileGroupBacklog.printBucketsKeys(_lastProcessesKeysBuckets);
    }

    @Override
    public String toString() {
        return toLogMessage();
    }


}
