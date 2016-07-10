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

package com.gigaspaces.internal.client.spaceproxy.actioninfo;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ISpaceProxyTypeManager;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.j_spaces.core.client.ReadModifiers;

import net.jini.core.transaction.Transaction;

public abstract class CommonProxyActionInfo implements Cloneable {
    public Transaction txn;
    public int modifiers;

    protected CommonProxyActionInfo(Transaction txn, int modifiers) {
        this.txn = txn;
        this.modifiers = modifiers;
    }

    @Override
    public CommonProxyActionInfo clone() {
        try {
            CommonProxyActionInfo copy = (CommonProxyActionInfo) super.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone a cloneable class", e);
        }
    }

    protected void initOperationId(ISpaceProxy spaceProxy, ITransportPacket packet) {
        if (packet.getOperationID() == null)
            packet.setOperationID(spaceProxy.createNewOperationID());
    }

    protected IEntryPacket toEntryPacket(ISpaceProxy spaceProxy, Object entry, ObjectType objectType) {
        ISpaceProxyTypeManager typeManager = spaceProxy.getDirectProxy().getTypeManager();
        IEntryPacket packet = typeManager.getEntryPacketFromObject(entry, objectType);
        initOperationId(spaceProxy, packet);
        return packet;
    }

    protected void validateReadModifiers(ISpaceProxy spaceProxy) {
        if (ReadModifiers.isExclusiveReadLock(modifiers) && txn == null && spaceProxy.getContextTransaction() == null)
            throw new IllegalArgumentException("Using EXCLUSIVE_READ_LOCK modifier without a transaction is illegal.");
    }


}
