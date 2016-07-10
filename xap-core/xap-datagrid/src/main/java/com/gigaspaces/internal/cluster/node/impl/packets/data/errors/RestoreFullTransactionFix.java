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

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFixFacade;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.TransactionOnePhaseReplicationPacketData;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class RestoreFullTransactionFix
        extends AbstractDataConsumeFix {

    private static final long serialVersionUID = 1L;
    private TransactionOnePhaseReplicationPacketData _restoredTransaction;

    public RestoreFullTransactionFix() {
    }

    public RestoreFullTransactionFix(
            TransactionOnePhaseReplicationPacketData restoredTransaction) {
        _restoredTransaction = restoredTransaction;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _restoredTransaction);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _restoredTransaction = IOUtils.readObject(in);
    }

    @Override
    public IExecutableReplicationPacketData<?> fix(
            IReplicationInContext context, IDataConsumeFixFacade fixFacade,
            ReplicationPacketDataConsumer consumer,
            IExecutableReplicationPacketData<?> data) throws Exception {
        return _restoredTransaction;
    }

    @Override
    public String toString() {
        return "RestoreFullTransactionFix - " + _restoredTransaction.toString() + "]";
    }

}
