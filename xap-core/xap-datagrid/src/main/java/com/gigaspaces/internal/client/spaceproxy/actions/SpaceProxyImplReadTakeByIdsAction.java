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

import com.gigaspaces.client.ReadTakeByIdsException;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeByIdsProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesByIdsSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesByIdsSpaceOperationResult;
import com.gigaspaces.internal.transport.IEntryPacket;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author idan
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplReadTakeByIdsAction extends ReadTakeByIdsProxyAction<SpaceProxyImpl> {
    @Override
    public Object[] readByIds(SpaceProxyImpl spaceProxy, ReadTakeByIdsProxyActionInfo actionInfo, boolean returnPackets)
            throws RemoteException, TransactionException, UnusableEntryException, InterruptedException {
        return readTakeByIds(spaceProxy, actionInfo, returnPackets);
    }

    @Override
    public Object[] takeByIds(SpaceProxyImpl spaceProxy, ReadTakeByIdsProxyActionInfo actionInfo, boolean returnPackets)
            throws RemoteException, TransactionException, UnusableEntryException, InterruptedException {
        return readTakeByIds(spaceProxy, actionInfo, returnPackets);
    }

    private Object[] readTakeByIds(SpaceProxyImpl spaceProxy, ReadTakeByIdsProxyActionInfo actionInfo, boolean returnPackets)
            throws InterruptedException, RemoteException, TransactionException, UnusableEntryException {
        try {
            IEntryPacket[] packets;

            if (actionInfo.ids.length == 0)
                packets = new IEntryPacket[0];
            else {
                final ReadTakeEntriesByIdsSpaceOperationRequest request = new ReadTakeEntriesByIdsSpaceOperationRequest(
                        actionInfo.queryPacket,
                        actionInfo.isTake,
                        actionInfo.modifiers,
                        actionInfo.txn);
                spaceProxy.beforeSpaceAction(actionInfo);
                spaceProxy.getProxyRouter().execute(request);
                final ReadTakeEntriesByIdsSpaceOperationResult result = request.getFinalResult();
                packets = result.getEntryPackets();
            }
            return actionInfo.convertResults(spaceProxy, packets, returnPackets, null);
        } catch (ReadTakeByIdsException e) {
            throw actionInfo.convertResults(spaceProxy, e, returnPackets, null);
        }
    }
}
