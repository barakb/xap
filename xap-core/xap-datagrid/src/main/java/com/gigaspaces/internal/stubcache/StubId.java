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

package com.gigaspaces.internal.stubcache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A unique identifier for an exported stub
 *
 * @author eitany
 * @since 7.5
 */
@com.gigaspaces.api.InternalApi
public class StubId implements Externalizable {
    private static final long serialVersionUID = 1L;

    public long _remoteLrmiRuntimeId;
    public long _remoteObjId;

    //Externalizable
    public StubId() {
    }

    public StubId(long remoteLrmiRuntimeId, long remoteObjId) {
        _remoteLrmiRuntimeId = remoteLrmiRuntimeId;
        _remoteObjId = remoteObjId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof StubId))
            return false;

        StubId other = (StubId) obj;
        return other._remoteLrmiRuntimeId == _remoteLrmiRuntimeId && other._remoteObjId == _remoteObjId;
    }

    @Override
    public int hashCode() {
        return (int) (_remoteObjId ^ (_remoteObjId >>> 32));
    }

    @Override
    public String toString() {
        return "Remote Lrmi Runtime Id=" + _remoteLrmiRuntimeId + ", Remote Object Id=" + _remoteObjId;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _remoteLrmiRuntimeId = in.readLong();
        _remoteObjId = in.readLong();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_remoteLrmiRuntimeId);
        out.writeLong(_remoteObjId);
    }
}
