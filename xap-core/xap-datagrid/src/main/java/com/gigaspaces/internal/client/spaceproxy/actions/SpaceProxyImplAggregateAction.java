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
import com.gigaspaces.internal.client.spaceproxy.actioninfo.AggregateProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.operations.AggregateEntriesSpaceOperationRequest;
import com.gigaspaces.query.aggregators.AggregationInternalUtils;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplAggregateAction extends AggregateProxyAction<SpaceProxyImpl> {

    @Override
    public AggregationResult aggregate(SpaceProxyImpl spaceProxy, AggregateProxyActionInfo actionInfo)
            throws RemoteException, TransactionException, InterruptedException {

        spaceProxy.beforeSpaceAction(actionInfo);

        if (actionInfo.isSqlQuery)
            actionInfo.queryPacket = spaceProxy.getQueryManager().getSQLTemplate((SQLQueryTemplatePacket) actionInfo.queryPacket, actionInfo.txn);

        List<SpaceEntriesAggregator> aggregators = AggregationInternalUtils.getAggregators(actionInfo.aggregationSet);
        AggregateEntriesSpaceOperationRequest request = new AggregateEntriesSpaceOperationRequest(actionInfo.queryPacket,
                actionInfo.txn,
                actionInfo.modifiers,
                aggregators);

        spaceProxy.getProxyRouter().execute(request);
        return request.getFinalResult(spaceProxy, actionInfo.queryPacket, actionInfo.isReturnPacket());
    }
}
