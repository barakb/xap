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

package com.gigaspaces.internal.cluster.node.impl.packets.data.errors;

import com.gigaspaces.internal.cluster.node.impl.handlers.UnknownEntryLeaseException;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeErrorResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataProducer;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A consumption error result that is caused when trying to extend entry lease and that lease do no
 * exists in the space.
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class UnknownEntryLeaseConsumeResult
        extends AbstractDataConsumeErrorResult {
    private static final long serialVersionUID = 1L;
    private String _className;
    private String _uid;
    private OperationID _operationID;
    private transient UnknownEntryLeaseException _error;

    public UnknownEntryLeaseConsumeResult() {
    }

    public UnknownEntryLeaseConsumeResult(String className, String uid, OperationID operationId, UnknownEntryLeaseException error) {
        _className = className;
        _uid = uid;
        _operationID = operationId;
        _error = error;
    }

    @Override
    public AbstractDataConsumeFix createFix(SpaceEngine spaceEngine,
                                            ReplicationPacketDataProducer producer, IExecutableReplicationPacketData errorData) {
        Context context = spaceEngine.getCacheManager().getCacheContext();
        try {
            IEntryHolder entryHolder = spaceEngine.getCacheManager().getEntry(context, _uid, _className, null, false, false, false);
            //Check for corresponding entry
            if (entryHolder == null || entryHolder.isDeleted() ||
                    entryHolder.isExpired())
                return new UnknownLeaseErrorUnfixed(_className, _uid);

            //If so ressurect it at the target
            IEntryPacket entryPacket = EntryPacketFactory.createFullPacketForReplication(entryHolder, _operationID);
            entryPacket.setSerializeTypeDesc(true);
            final long expirationTime = entryHolder.getEntryData().getExpirationTime();
            if (expirationTime != Long.MAX_VALUE) {
                long ttl = expirationTime - SystemTime.timeMillis();
                if (ttl <= 0)
                    return new UnknownLeaseErrorUnfixed(_className, _uid);
                entryPacket.setTTL(ttl);
            }
            //We want to have this entry packet inserted with version 0 so if there are
            //pending update operation in the redo log of this packet they will execute without
            //version conflict. If this case is true, there must be an update replication packet
            //of this entry packet pending as well, so eventually the state will be consistent.
            entryPacket.setVersion(0);
            return new ResurrectEntryFix(entryPacket);
        } catch (SAException e) {
            return new UnknownLeaseErrorUnfixed(_className, _uid);
        } finally {
            spaceEngine.getCacheManager().freeCacheContext(context);
        }
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _className = IOUtils.readString(in);
        _uid = IOUtils.readString(in);
        _operationID = IOUtils.readObject(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _className);
        IOUtils.writeString(out, _uid);
        IOUtils.writeObject(out, _operationID);
    }

    public Exception toException() {
        return _error;
    }

    public boolean sameFailure(IDataConsumeResult otherResult) {
        //This failure cannot reoccur
        return false;
    }

}
