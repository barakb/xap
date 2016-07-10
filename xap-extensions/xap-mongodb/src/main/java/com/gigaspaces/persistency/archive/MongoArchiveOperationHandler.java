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

package com.gigaspaces.persistency.archive;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.persistency.MongoClientConnector;
import com.gigaspaces.persistency.MongoClientConnectorConfigurer;
import com.gigaspaces.persistency.error.SpaceMongoException;
import com.gigaspaces.persistency.metadata.BatchUnit;
import com.gigaspaces.sync.DataSyncOperationType;
import com.mongodb.MongoClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.archive.ArchiveOperationHandler;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author Shadi Massalha
 */
public class MongoArchiveOperationHandler implements ArchiveOperationHandler {

    private static final Log logger = LogFactory.getLog(MongoArchiveOperationHandler.class);

    // injected(required)
    private GigaSpace gigaSpace;

    private MongoClientConnector connector;

    private MongoClient client;

    private String db;

    @Required
    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    /**
     * @throws SpaceMongoException - Problem encountered while archiving to mongodb
     * @see ArchiveOperationHandler#archive(Object...)
     */
    public void archive(Object... objects) {

        List<BatchUnit> rows = new ArrayList<BatchUnit>(objects.length);

        for (Object object : objects) {

            if (!(object instanceof SpaceDocument)) {
                throw new SpaceMongoArchiveOperationHandlerException(
                        object.getClass()
                                + " is not supported since it is not a "
                                + SpaceDocument.class.getName());
            }

            BatchUnit batchUnit = new BatchUnit();

            batchUnit.setSpaceDocument((SpaceDocument) object);
            ((SpaceDocument) object).getTypeName();
            batchUnit.setDataSyncOperationType(DataSyncOperationType.WRITE);

            rows.add(batchUnit);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Writing to mongo " + rows.size() + " objects");
        }
        // TODO: check if type descriptor is empty gigaspace ref
        connector.performBatch(rows);
    }

    /**
     * @return true - Since Multiple archiving of the exact same objects is supported (idempotent).
     * @see ArchiveOperationHandler#supportsBatchArchiving()
     */
    public boolean supportsBatchArchiving() {
        return true;
    }

    @PostConstruct
    public void afterPropertiesSet() {

        if (gigaSpace == null) {
            throw new IllegalArgumentException("gigaSpace cannot be null");
        }

        createMongoClient();
    }

    private void createMongoClient() {

        connector = new MongoClientConnectorConfigurer().client(client).db(db)
                .create();
    }

    @SuppressWarnings("UnusedDeclaration")
    public GigaSpace getGigaSpace() {
        return gigaSpace;
    }

    /**
     * @param db Mongo database name.
     * @see MongoClientConnectorConfigurer#db(String)
     */
    @Required
    @SuppressWarnings("SpellCheckingInspection")
    public void setDb(String db) {
        this.db = db;

    }

    /**
     * @param client Mongo database client.
     * @see MongoClientConnectorConfigurer#client(MongoClient)
     */
    @SuppressWarnings("SpellCheckingInspection")
    @Required
    public void setConfig(MongoClient client) {
        this.client = client;
    }

    @PreDestroy
    @SuppressWarnings("UnusedDeclaration")
    public void destroy() {
        if (connector != null) {
            try {
                connector.close();
            } catch (IOException e) {
                throw new SpaceMongoArchiveOperationHandlerException(
                        "can not close mongo client", e);
            }
        }
    }

    public MongoClient getConfig() {
        return client;
    }
}
