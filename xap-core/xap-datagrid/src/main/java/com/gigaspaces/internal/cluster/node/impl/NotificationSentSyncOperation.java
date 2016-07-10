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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationUnreliableOperation;
import com.j_spaces.core.OperationID;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class NotificationSentSyncOperation
        implements IReplicationUnreliableOperation {

    private static final long serialVersionUID = 1L;
    private OperationID _operationId;

    public NotificationSentSyncOperation() {
    }


    public NotificationSentSyncOperation(OperationID operationId) {
        _operationId = operationId;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        _operationId.writeExternal(out);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _operationId = new OperationID();
        _operationId.readExternal(in);
    }

    public void execute(String sourceMemberLookupName,
                        IReplicationInFacade replicationInFacade) throws Exception {
        replicationInFacade.inNotificationSent(new ReplicationInContext(sourceMemberLookupName, null, null, false, false), _operationId);
    }

    public Type getOperationType() {
        return Type.NotificationSent;
    }


    @Override
    public String toString() {
        return "NOTIFICATION SENT (" + _operationId + ")";
    }
}
