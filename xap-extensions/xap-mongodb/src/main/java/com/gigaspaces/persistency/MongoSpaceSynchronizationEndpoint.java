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

package com.gigaspaces.persistency;

import com.gigaspaces.sync.AddIndexData;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.IntroduceTypeData;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.gigaspaces.sync.TransactionData;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * A MongoDB implementation of  {@link SpaceSynchronizationEndpoint }
 *
 * @author Shadi Massalha
 */
public class MongoSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint {

    private static final Log logger = LogFactory.getLog(MongoSpaceSynchronizationEndpoint.class);

    private final MongoClientConnector client;

    public MongoSpaceSynchronizationEndpoint(MongoClientConnector client) {

        if (client == null)
            throw new IllegalArgumentException("mongo client can not be null.");
        this.client = client;
    }

    public void close() throws IOException {
        if (logger.isDebugEnabled())
            logger.trace("MongoSpaceSynchronizationEndpoint.close()");

        client.close();
    }

    @Override
    public void onIntroduceType(IntroduceTypeData introduceTypeData) {

        if (logger.isDebugEnabled())
            logger.trace("MongoSpaceSynchronizationEndpoint.onIntroduceType(" + introduceTypeData + ")");

        client.introduceType(introduceTypeData);
    }

    @Override
    public void onAddIndex(AddIndexData addIndexData) {
        if (logger.isDebugEnabled())
            logger.debug("MongoSpaceSynchronizationEndpoint.onAddIndex(" + addIndexData + ")");

        client.ensureIndexes(addIndexData);
    }

    @Override
    public void onOperationsBatchSynchronization(OperationsBatchData batchData) {

        if (logger.isDebugEnabled())
            logger.debug("MongoSpaceSynchronizationEndpoint.onOperationsBatchSynchronization()");

        synchronize(batchData.getBatchDataItems());
    }

    @Override
    public void onTransactionSynchronization(TransactionData transactionData) {

        if (logger.isDebugEnabled())
            logger.debug("MongoSpaceSynchronizationEndpoint.onTransactionSynchronization()");

        synchronize(transactionData.getTransactionParticipantDataItems());
    }

    private void synchronize(DataSyncOperation operations[]) {
        if (logger.isDebugEnabled())
            logger.trace("MongoSpaceSynchronizationEndpoint.doSynchronization()");

        client.performBatch(operations);
    }
}
