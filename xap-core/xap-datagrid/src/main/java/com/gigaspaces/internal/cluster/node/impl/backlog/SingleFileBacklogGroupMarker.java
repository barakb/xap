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
import java.util.Arrays;


/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class SingleFileBacklogGroupMarker
        implements IMarker {

    private final AbstractSingleFileGroupBacklog<?, ?> _groupBacklog;
    private final String[] _memberNames;
    private final long _markedKey;

    public SingleFileBacklogGroupMarker(AbstractSingleFileGroupBacklog<?, ?> groupBacklog, String[] memberNames,
                                        long markedKey) {
        _groupBacklog = groupBacklog;
        _memberNames = memberNames;
        _markedKey = markedKey;
    }

    @Override
    public boolean isMarkerReached() {
        for (String memberName : _memberNames) {
            if (!_groupBacklog.isMarkerReached(memberName, _markedKey))
                return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "members=" + Arrays.toString(_memberNames) + " key=" + _markedKey;
    }

    @Override
    public IMarkerWireForm toWireForm() {
        return new SingleFileBacklogGroupMarkerWireForm(_groupBacklog.getGroupName(), _memberNames, _markedKey);
    }

    public static class SingleFileBacklogGroupMarkerWireForm
            implements IMarkerWireForm {

        private static final long serialVersionUID = 1L;

        private String _groupName;
        private String[] _memberNames;
        private long _markedKey;

        public SingleFileBacklogGroupMarkerWireForm() {
        }

        public SingleFileBacklogGroupMarkerWireForm(String groupName, String[] memberNames, long markedKey) {
            _groupName = groupName;
            _memberNames = memberNames;
            _markedKey = markedKey;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeRepetitiveString(out, _groupName);
            IOUtils.writeRepetitiveStringArray(out, _memberNames);
            out.writeLong(_markedKey);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            _groupName = IOUtils.readRepetitiveString(in);
            _memberNames = IOUtils.readRepetitiveStringArray(in);
            _markedKey = in.readLong();
        }

        @Override
        public IMarker toFinalizedForm(IReplicationGroupBacklog backlog) {
            return new SingleFileBacklogGroupMarker((AbstractSingleFileGroupBacklog<?, ?>) backlog, _memberNames, _markedKey);
        }

        @Override
        public String getGroupName() {
            return _groupName;
        }

        @Override
        public String toString() {
            return "members=" + Arrays.toString(_memberNames) + " key=" + _markedKey;
        }

    }
}
