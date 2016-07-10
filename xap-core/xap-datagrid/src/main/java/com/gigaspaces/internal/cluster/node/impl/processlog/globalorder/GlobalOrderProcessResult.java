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

package com.gigaspaces.internal.cluster.node.impl.processlog.globalorder;

import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class GlobalOrderProcessResult implements IProcessResult {
    private static final long serialVersionUID = 1L;

    public static final GlobalOrderProcessResult OK = new GlobalOrderProcessResult(true);

    private boolean _processed;
    private Throwable _error;
    private long _lastProcessedKey;

    //Externalizable
    public GlobalOrderProcessResult() {
    }

    public GlobalOrderProcessResult(boolean processed) {
        _processed = processed;
    }

    public GlobalOrderProcessResult(Throwable error, long lastProcessedKey) {
        _processed = false;
        _error = error;
        _lastProcessedKey = lastProcessedKey;
    }

    public boolean isProcessed() {
        return _processed;
    }

    public long getLastProcessedKey() {
        return _lastProcessedKey;
    }

    public Throwable getError() {
        return _error;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _processed = in.readBoolean();
        if (!_processed) {
            _error = IOUtils.readObject(in);
            _lastProcessedKey = in.readLong();
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(_processed);
        if (!_processed) {
            IOUtils.writeObject(out, _error);
            out.writeLong(_lastProcessedKey);
        }
    }

    @Override
    public String toString() {
        return "GlobalOrderProcessResult [processed=" + _processed + ", lastProcessedKey=" + _lastProcessedKey + ", error=" + _error + "]";
    }

}
