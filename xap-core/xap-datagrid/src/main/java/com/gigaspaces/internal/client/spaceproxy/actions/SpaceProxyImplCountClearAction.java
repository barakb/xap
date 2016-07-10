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

import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.CountClearProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.operations.CountClearEntriesSpaceOperationRequest;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplCountClearAction extends CountClearProxyAction<SpaceProxyImpl> {
    @Override
    public int execute(SpaceProxyImpl spaceProxy, CountClearProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException, TransactionException {
        if (actionInfo.isSqlQuery)
            return spaceProxy.getQueryManager().countClear(actionInfo);

        try {
            spaceProxy.beforeSpaceAction(actionInfo);

            final CountClearEntriesSpaceOperationRequest request = new CountClearEntriesSpaceOperationRequest(
                    actionInfo.queryPacket, actionInfo.txn, actionInfo.isTake, actionInfo.modifiers);
            spaceProxy.getProxyRouter().execute(request);
            return request.getFinalResult();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex.toString());
        }
    }
}
