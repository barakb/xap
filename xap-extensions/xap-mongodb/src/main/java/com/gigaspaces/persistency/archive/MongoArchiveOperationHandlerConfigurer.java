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

import com.mongodb.MongoClient;

import org.openspaces.core.GigaSpace;


/**
 * @author Shadi Massalha
 */
@SuppressWarnings("UnusedDeclaration")
public class MongoArchiveOperationHandlerConfigurer {

    MongoArchiveOperationHandler handler;
    private boolean initialized;

    public MongoArchiveOperationHandlerConfigurer() {
        handler = new MongoArchiveOperationHandler();
    }

    /**
     * @param db a name of Mongo database
     * @return this
     * @see MongoArchiveOperationHandler#setDb(String)
     */
    @SuppressWarnings("SpellCheckingInspection")
    public MongoArchiveOperationHandlerConfigurer db(String db) {
        handler.setDb(db);
        return this;
    }

    /**
     * @param client a MongoClient
     * @return this
     * @see MongoArchiveOperationHandler#setConfig(MongoClient)
     */
    @SuppressWarnings("SpellCheckingInspection")
    public MongoArchiveOperationHandlerConfigurer config(MongoClient client) {
        handler.setConfig(client);
        return this;
    }

    /**
     * @param gigaSpace An instance of GigaSpace
     * @return this
     * @see MongoArchiveOperationHandler#setGigaSpace(GigaSpace)
     */
    public MongoArchiveOperationHandlerConfigurer gigaSpace(GigaSpace gigaSpace) {
        handler.setGigaSpace(gigaSpace);
        return this;
    }

    public MongoArchiveOperationHandler create() {

        if (!initialized) {

            handler.afterPropertiesSet();

            initialized = true;
        }

        return handler;
    }

}
