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

package com.gigaspaces.internal.cluster.node.impl.backlog;

import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarkerWireForm;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class SingleFileBacklogMarker
        implements IMarker {

    private final AbstractSingleFileGroupBacklog<?, ?> _groupBacklog;
    private final String _memberName;
    private final long _markedKey;

    public SingleFileBacklogMarker(AbstractSingleFileGroupBacklog<?, ?> groupBacklog,
                                   String memberName, long markedKey) {
        _groupBacklog = groupBacklog;
        _memberName = memberName;
        _markedKey = markedKey;
    }

    public boolean isMarkerReached() {
        return _groupBacklog.isMarkerReached(_memberName, _markedKey);
    }

    @Override
    public IMarkerWireForm toWireForm() {
        return new SingleFileBacklogMarkerWireForm(_groupBacklog.getGroupName(), _memberName, _markedKey);
    }

    @Override
    public String toString() {
        return "member=" + _memberName + " key=" + _markedKey;
    }

    public static class SingleFileBacklogMarkerWireForm
            implements IMarkerWireForm {
        private static final long serialVersionUID = 1L;

        private String _groupName;
        private String _memberName;
        private long _markedKey;

        public SingleFileBacklogMarkerWireForm() {
        }

        public SingleFileBacklogMarkerWireForm(String groupName, String memberName, long markedKey) {
            _groupName = groupName;
            _memberName = memberName;
            _markedKey = markedKey;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeRepetitiveString(out, _groupName);
            IOUtils.writeRepetitiveString(out, _memberName);
            out.writeLong(_markedKey);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            _groupName = IOUtils.readRepetitiveString(in);
            _memberName = IOUtils.readRepetitiveString(in);
            _markedKey = in.readLong();
        }

        @Override
        public IMarker toFinalizedForm(IReplicationGroupBacklog backlog) {
            return new SingleFileBacklogMarker((AbstractSingleFileGroupBacklog<?, ?>) backlog, _memberName, _markedKey);
        }

        @Override
        public String getGroupName() {
            return _groupName;
        }

        @Override
        public String toString() {
            return "member=" + _memberName + " key=" + _markedKey;
        }
    }
}
