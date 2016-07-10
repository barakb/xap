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

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ChangeProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.operations.ChangeEntriesSpaceOperationRequest;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.concurrent.Future;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplChangeAction
        extends ChangeProxyAction<SpaceProxyImpl> {

    @Override
    public ChangeResult<?> change(SpaceProxyImpl spaceProxy,
                                  ChangeProxyActionInfo actionInfo) throws InterruptedException, RemoteException, TransactionException {
        spaceProxy.beforeSpaceAction(actionInfo);

        if (actionInfo.isSqlQuery)
            actionInfo.queryPacket = spaceProxy.getQueryManager().getSQLTemplate((SQLQueryTemplatePacket) actionInfo.queryPacket, actionInfo.txn);

        ChangeEntriesSpaceOperationRequest request = new ChangeEntriesSpaceOperationRequest(actionInfo.queryPacket,
                actionInfo.txn,
                actionInfo.timeout,
                actionInfo.lease,
                actionInfo.modifiers,
                actionInfo.mutators,
                actionInfo.getQuery());
        if (Modifiers.contains(actionInfo.modifiers, Modifiers.ONE_WAY)) {
            spaceProxy.getProxyRouter().executeOneway(request);
            return null;
        }
        spaceProxy.getProxyRouter().execute(request);
        return request.getFinalResult();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Future<ChangeResult<T>> asyncChange(SpaceProxyImpl spaceProxy,
                                                   ChangeProxyActionInfo actionInfo,
                                                   @SuppressWarnings("rawtypes") AsyncFutureListener listener) {
        spaceProxy.beforeSpaceAction(actionInfo);

        if (actionInfo.isSqlQuery)
            actionInfo.queryPacket = spaceProxy.getQueryManager().getSQLTemplate((SQLQueryTemplatePacket) actionInfo.queryPacket, actionInfo.txn);

        ChangeEntriesSpaceOperationRequest request = new ChangeEntriesSpaceOperationRequest(actionInfo.queryPacket,
                actionInfo.txn,
                actionInfo.timeout,
                actionInfo.lease,
                actionInfo.modifiers,
                actionInfo.mutators,
                actionInfo.getQuery());
        if (Modifiers.contains(actionInfo.modifiers, Modifiers.ONE_WAY))
            throw new UnsupportedOperationException("Oneway operation contradicts asynchronous invocation");

        return spaceProxy.getProxyRouter().executeAsync(request, listener);
    }

}
