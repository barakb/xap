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

import com.gigaspaces.internal.cluster.node.impl.handlers.UnknownNotifyTemplateLeaseException;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeErrorResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataProducer;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.time.SystemTime;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A consumption error result that is caused when trying to extend notify template lease and that
 * lease do no exists in the space.
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class UnknownNotifyTemplateLeaseConsumeResult
        extends AbstractDataConsumeErrorResult {
    private static final long serialVersionUID = 1L;
    private String _className;
    private String _uid;
    private transient UnknownNotifyTemplateLeaseException _error;

    public UnknownNotifyTemplateLeaseConsumeResult() {
    }

    public UnknownNotifyTemplateLeaseConsumeResult(String className, String uid, UnknownNotifyTemplateLeaseException error) {
        _className = className;
        _uid = uid;
        _error = error;
    }

    @Override
    public AbstractDataConsumeFix createFix(SpaceEngine spaceEngine,
                                            ReplicationPacketDataProducer producer, IExecutableReplicationPacketData errorData) {
        NotifyTemplateHolder templateHolder = (NotifyTemplateHolder) spaceEngine.getCacheManager().getTemplate(_uid);
        //Check if there is a corresponding notify template
        if (templateHolder == null || templateHolder.isDeleted() ||
                templateHolder.isExpired())
            return new UnknownLeaseErrorUnfixed(_className, _uid);
        //If so ressurect it at the target
        ITemplatePacket templatePacket = TemplatePacketFactory.createFullPacketForReplication(templateHolder,
                templateHolder.getOperationID());
        templatePacket.setSerializeTypeDesc(true);
        final long expirationTime = templateHolder.getEntryData()
                .getExpirationTime();
        if (expirationTime != Long.MAX_VALUE) {
            long ttl = expirationTime - SystemTime.timeMillis();
            if (ttl <= 0)
                return new UnknownLeaseErrorUnfixed(_className, _uid);
            templatePacket.setTTL(ttl);
        }
        return new ResurrectNotifyTemplateFix(templatePacket, _uid, templateHolder.getNotifyInfo());
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _className = IOUtils.readString(in);
        _uid = IOUtils.readString(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _className);
        IOUtils.writeString(out, _uid);
    }

    public Exception toException() {
        return _error;
    }

    public boolean sameFailure(IDataConsumeResult otherResult) {
        //This error cannot occur twice
        return false;
    }
}
