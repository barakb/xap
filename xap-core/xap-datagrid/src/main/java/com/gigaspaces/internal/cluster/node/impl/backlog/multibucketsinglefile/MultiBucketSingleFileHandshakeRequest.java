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

package com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileHandshakeRequest
        implements IBacklogHandshakeRequest {

    private static final long serialVersionUID = 1L;

    private long[] _bucketsConfirmedKeys;
    private long _globalConfirmedKey;
    private boolean _firstHandshake;

    public MultiBucketSingleFileHandshakeRequest(long globalConfirmedKey,
                                                 long[] bucketsConfirmedKeys, boolean firstHandshake) {
        _globalConfirmedKey = globalConfirmedKey;
        _firstHandshake = firstHandshake;
        _bucketsConfirmedKeys = new long[bucketsConfirmedKeys.length];
        for (int i = 0; i < bucketsConfirmedKeys.length; i++) {
            _bucketsConfirmedKeys[i] = bucketsConfirmedKeys[i];
        }
    }

    public MultiBucketSingleFileHandshakeRequest() {

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(_firstHandshake);
        out.writeLong(_globalConfirmedKey);
        IOUtils.writeLongArray(out, _bucketsConfirmedKeys);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _firstHandshake = in.readBoolean();
        _globalConfirmedKey = in.readLong();
        _bucketsConfirmedKeys = IOUtils.readLongArray(in);
    }

    public boolean isFirstHandshake() {
        return _firstHandshake;
    }

    public long[] getBucketsConfirmedKeys() {
        return _bucketsConfirmedKeys;
    }

    public long getGlobalConfirmedKey() {
        return _globalConfirmedKey;
    }

    public String toLogMessage() {
        return "Handshake request. First handshake [" + _firstHandshake + "]. Last global confirmed key by source [" + getGlobalConfirmedKey() + "] Last confirmed bucket keys by source " + AbstractMultiBucketSingleFileGroupBacklog.printBucketsKeys(_bucketsConfirmedKeys) + "]";
    }

    @Override
    public String toString() {
        return toLogMessage();
    }

}
