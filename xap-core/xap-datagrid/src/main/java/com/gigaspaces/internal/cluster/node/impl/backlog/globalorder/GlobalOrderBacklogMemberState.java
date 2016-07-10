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

package com.gigaspaces.internal.cluster.node.impl.backlog.globalorder;

import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class GlobalOrderBacklogMemberState
        implements IBacklogMemberState {
    private static final long serialVersionUID = 1L;

    private String _memberName;
    private boolean _hadAnyHandshake;
    private long _lastConfirmedKey;
    private boolean _backlogDropped;
    private Throwable _inconsistencyReason;

    public GlobalOrderBacklogMemberState() {
    }

    public GlobalOrderBacklogMemberState(String memberName,
                                         boolean hadAnyHandshake, long lastConfirmedKey,
                                         boolean backlogDropped, Throwable inconsistencyReason) {
        _memberName = memberName;
        _hadAnyHandshake = hadAnyHandshake;
        _lastConfirmedKey = lastConfirmedKey;
        _backlogDropped = backlogDropped;
        _inconsistencyReason = inconsistencyReason;
    }

    @Override
    public boolean isExistingMember() {
        return true;
    }

    public boolean isBacklogDropped() {
        return _backlogDropped;
    }

    public String toLogMessage() {
        return "Member [" + _memberName + "] Had any handshake ["
                + _hadAnyHandshake + "] Last confirmed key ["
                + getLastConfirmedKey() + "] Backlog dropped ["
                + _backlogDropped + "]";
    }

    public long getLastConfirmedKey() {
        return _lastConfirmedKey;
    }

    public boolean isInconsistent() {
        return _inconsistencyReason != null;
    }

    public Throwable getInconsistencyReason() {
        return _inconsistencyReason;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeRepetitiveString(out, _memberName);
        out.writeBoolean(_hadAnyHandshake);
        out.writeLong(_lastConfirmedKey);
        out.writeBoolean(_backlogDropped);
        IOUtils.writeObject(out, _inconsistencyReason);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _memberName = IOUtils.readRepetitiveString(in);
        _hadAnyHandshake = in.readBoolean();
        _lastConfirmedKey = in.readLong();
        _backlogDropped = in.readBoolean();
        _inconsistencyReason = IOUtils.readObject(in);
    }

}
