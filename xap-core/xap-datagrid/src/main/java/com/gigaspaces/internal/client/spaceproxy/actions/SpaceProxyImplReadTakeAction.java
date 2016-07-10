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

package com.gigaspaces.internal.client.spaceproxy.actions;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.cluster.replication.TakeConsistencyLevelCompromisedException;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeAsyncProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.metadata.SpaceProxyTypeManager;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntrySpaceOperationRequest;
import com.gigaspaces.internal.transport.IEntryPacket;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplReadTakeAction extends ReadTakeProxyAction<SpaceProxyImpl> {
    @Override
    public Object read(SpaceProxyImpl spaceProxy, ReadTakeProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return readTake(spaceProxy, actionInfo);
    }

    @Override
    public Object take(SpaceProxyImpl spaceProxy, ReadTakeProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return readTake(spaceProxy, actionInfo);
    }

    private Object readTake(SpaceProxyImpl spaceProxy, ReadTakeProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        IEntryPacket result;

        spaceProxy.beforeSpaceAction(actionInfo);

        if (actionInfo.isSqlQuery)
            result = spaceProxy.getQueryManager().readTake(actionInfo);
        else {
            final ReadTakeEntrySpaceOperationRequest request = new ReadTakeEntrySpaceOperationRequest(
                    actionInfo.queryPacket,
                    actionInfo.txn,
                    actionInfo.isTake,
                    actionInfo.ifExists,
                    actionInfo.timeout,
                    actionInfo.modifiers,
                    actionInfo.returnOnlyUids,
                    actionInfo.getQuery());
            spaceProxy.getProxyRouter().execute(request);
            result = request.getFinalResult();
            if (actionInfo.isTake && request.getRemoteOperationResult().getSyncReplicationLevel() + 1 < SpaceProxyTypeManager.requiredConsistencyLevel()) {
                throw new TakeConsistencyLevelCompromisedException(request.getRemoteOperationResult().getSyncReplicationLevel() + 1, actionInfo.convertQueryResult(spaceProxy, result, null));
            }
        }
        return actionInfo.convertQueryResult(spaceProxy, result, null);
    }

    @Override
    public AsyncFuture<?> asyncRead(SpaceProxyImpl spaceProxy, ReadTakeAsyncProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return asyncReadTake(spaceProxy, actionInfo);
    }

    @Override
    public AsyncFuture<?> asyncTake(SpaceProxyImpl spaceProxy, ReadTakeAsyncProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return asyncReadTake(spaceProxy, actionInfo);
    }

    private AsyncFuture<?> asyncReadTake(SpaceProxyImpl spaceProxy, ReadTakeAsyncProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        spaceProxy.beforeSpaceAction(actionInfo);

        final ReadTakeEntrySpaceOperationRequest request = new ReadTakeEntrySpaceOperationRequest(
                actionInfo.queryPacket,
                actionInfo.txn,
                actionInfo.isTake,
                actionInfo.ifExists,
                actionInfo.timeout,
                actionInfo.modifiers,
                actionInfo.returnOnlyUids,
                spaceProxy.getTypeManager(),
                actionInfo.isReturnPacket(),
                actionInfo.getQuery());

        return spaceProxy.getProxyRouter().executeAsync(request, actionInfo.listener);
    }

}
