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

import com.gigaspaces.client.ReadMultipleException;
import com.gigaspaces.client.TakeMultipleException;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeMultipleProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesSpaceOperationRequest;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.client.ReadModifiers;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplReadTakeMultipleAction extends ReadTakeMultipleProxyAction<SpaceProxyImpl> {
    @SuppressWarnings("deprecation")
    @Override
    public Object[] readMultiple(SpaceProxyImpl spaceProxy, ReadTakeMultipleProxyActionInfo actionInfo)
            throws RemoteException, TransactionException, UnusableEntryException {
        Object[] results = null;
        try {
            results = actionInfo.convertQueryResults(spaceProxy, newReadTakeMultiple(spaceProxy, actionInfo), null);
        } catch (ReadMultipleException ex) {
            throw actionInfo.convertExceptionResults(spaceProxy, ex, null);
        } catch (Exception ex) {
            if (ReadModifiers.isThrowPartialFailure(actionInfo.modifiers))
                throw new ReadMultipleException(ex);
            rethrowException(ex);
        }
        return results;
    }

    @Override
    public Object[] takeMultiple(SpaceProxyImpl spaceProxy, ReadTakeMultipleProxyActionInfo actionInfo)
            throws RemoteException, TransactionException, UnusableEntryException {
        Object[] results = null;
        try {
            results = actionInfo.convertQueryResults(spaceProxy, newReadTakeMultiple(spaceProxy, actionInfo), null);

        } catch (TakeMultipleException ex) {
            throw actionInfo.convertExceptionResults(spaceProxy, ex, null);
        } catch (Exception ex) {
            if (ReadModifiers.isThrowPartialFailure(actionInfo.modifiers))
                throw new TakeMultipleException(ex);
            rethrowException(ex);
        }
        return results;
    }

    private IEntryPacket[] newReadTakeMultiple(SpaceProxyImpl spaceProxy, ReadTakeMultipleProxyActionInfo actionInfo)
            throws InterruptedException, RemoteException, TransactionException, UnusableEntryException {
        if (actionInfo.returnOnlyUids)
            throw new IllegalArgumentException("returnOnlyUids=true is not supported in new router read/takeMultiple operation");

        spaceProxy.beforeSpaceAction(actionInfo);

        if (actionInfo.maxResults < 1)
            return new IEntryPacket[0];

        if (actionInfo.isSqlQuery)
            return spaceProxy.getQueryManager().readTakeMultiple(actionInfo);

        final ReadTakeEntriesSpaceOperationRequest request = new ReadTakeEntriesSpaceOperationRequest(
                actionInfo.queryPacket,
                actionInfo.txn,
                actionInfo.isTake,
                actionInfo.modifiers,
                actionInfo.maxResults,
                actionInfo.minEntriesToWaitFor,
                actionInfo.timeout,
                actionInfo.ifExist,
                actionInfo.getQuery());
        spaceProxy.getProxyRouter().execute(request);
        if (actionInfo.isTake && request.getRemoteOperationResult() != null) {
            actionInfo.setSyncReplicationLevel(request.getRemoteOperationResult().getSyncReplicationLevel());
        } else if (actionInfo.isTake && request.getLevels() != null) {
            actionInfo.setSyncReplicationLevels(request.getLevels());
        }
        return request.getFinalResult();
    }
}
