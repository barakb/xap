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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;


public abstract class ReplicationGroupOutContext implements IReplicationGroupOutContext, Textualizable {

    private ReplicationOutContext _entireContext;
    private final String _name;

    public ReplicationGroupOutContext(String name) {
        _name = name;
    }

    public String getName() {
        return _name;
    }

    public ReplicationOutContext getEntireContext() {
        return _entireContext;
    }

    public void setEntireContext(ReplicationOutContext entireContext) {
        _entireContext = entireContext;
    }

    public void afterAddingOrderedPacket(IReplicationOrderedPacket packet) { //in case of direct persistency set redo key
        if (_entireContext != null && _entireContext.getDirectPesistencySyncHandler() != null)
            _entireContext.getDirectPesistencySyncHandler().afterInsertingToRedolog(_entireContext, packet.getKey());
    }


    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("name", getName());
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

}