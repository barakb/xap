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

import com.gigaspaces.internal.cluster.node.impl.processlog.sync.oneway.IProcessStatus;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class GlobalOrderProcessStatus
        implements IProcessStatus {
    private static final long serialVersionUID = 1L;

    private long _lastProcessedKey;
    private Throwable _error;

    public GlobalOrderProcessStatus() {
    }

    public GlobalOrderProcessStatus(long lastProcessedKey, Throwable error) {
        _lastProcessedKey = lastProcessedKey;
        _error = error;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_lastProcessedKey);
        IOUtils.writeObject(out, _error);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _lastProcessedKey = in.readLong();
        _error = IOUtils.readObject(in);
    }

    public long getLastProcessedKey() {
        return _lastProcessedKey;
    }

    public boolean hasPendingError() {
        return getError() != null;
    }

    public Throwable getError() {
        return _error;
    }

    public boolean isOk() {
        return !hasPendingError();
    }

    @Override
    public String toString() {
        return "GlobalOrderProcessStatus [getLastProcessedKey()="
                + getLastProcessedKey() + ", hasPendingError()="
                + hasPendingError() + ", getError()=" + getError()
                + ", isOk()=" + isOk() + "]";
    }


}
