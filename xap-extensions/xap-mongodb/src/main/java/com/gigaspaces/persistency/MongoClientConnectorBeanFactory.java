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

import com.mongodb.MongoClient;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;


/**
 * Default spring bean factory implementation that can get external {@link
 * MongoClientConnectorConfigurer} as configurer otherwise create it own one
 *
 * @author Shadi Massalha
 */
public class MongoClientConnectorBeanFactory implements
        FactoryBean<MongoClientConnector>, InitializingBean, DisposableBean {

    private MongoClientConnector mongoClientConnector;

    private final MongoClientConnectorConfigurer configurer = getConfigurer();

    public void setDb(String db) {
        configurer.db(db);
    }

    public void setConfig(MongoClient config) {
        configurer.client(config);
    }

    public void destroy() throws Exception {
        mongoClientConnector.close();
    }

    private MongoClientConnectorConfigurer getConfigurer() {
        return new MongoClientConnectorConfigurer();
    }

    public void afterPropertiesSet() throws Exception {
        this.mongoClientConnector = configurer.create();
    }

    public MongoClientConnector getObject() throws Exception {
        return mongoClientConnector;
    }

    public Class<?> getObjectType() {
        return MongoClientConnector.class;
    }

    public boolean isSingleton() {
        return true;
    }
}
