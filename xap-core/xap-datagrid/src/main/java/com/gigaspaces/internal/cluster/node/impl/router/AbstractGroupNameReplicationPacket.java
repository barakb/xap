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

package com.gigaspaces.internal.cluster.node.impl.router;

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroup;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class AbstractGroupNameReplicationPacket<T>
        extends AbstractReplicationPacket<T> {
    private static final long serialVersionUID = 1L;

    private String _groupName;

    public AbstractGroupNameReplicationPacket() {
    }

    public AbstractGroupNameReplicationPacket(String groupName) {
        super();
        _groupName = groupName;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeRepetitiveString(out, _groupName);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _groupName = IOUtils.readRepetitiveString(in);
    }

    public String getGroupName() {
        return _groupName;
    }

    protected IReplicationTargetGroup getTargetGroup(
            IIncomingReplicationFacade incomingReplicationFacade) {
        IReplicationTargetGroup targetGroup = incomingReplicationFacade.getReplicationTargetGroup(getGroupName());
        return targetGroup;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("groupName", getGroupName());
    }

}
