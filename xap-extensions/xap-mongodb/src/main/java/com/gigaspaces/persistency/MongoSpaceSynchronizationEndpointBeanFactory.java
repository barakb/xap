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

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

public class MongoSpaceSynchronizationEndpointBeanFactory implements
        FactoryBean<MongoSpaceSynchronizationEndpoint>, InitializingBean,
        DisposableBean {

    private final MongoSpaceSynchronizationEndpointConfigurer configurer = getConfigurer();
    private MongoSpaceSynchronizationEndpoint mongoSpaceSynchronizationEndpoint;

    public void setMongoClientConnector(
            MongoClientConnector mongoClientConnector) {
        configurer.mongoClientConnector(mongoClientConnector);
    }

    public void destroy() throws Exception {
        mongoSpaceSynchronizationEndpoint.close();
    }

    private MongoSpaceSynchronizationEndpointConfigurer getConfigurer() {
        return new MongoSpaceSynchronizationEndpointConfigurer();
    }

    public void afterPropertiesSet() throws Exception {
        this.mongoSpaceSynchronizationEndpoint = configurer.create();

    }

    public MongoSpaceSynchronizationEndpoint getObject() throws Exception {
        return mongoSpaceSynchronizationEndpoint;
    }

    public Class<?> getObjectType() {
        return MongoSpaceSynchronizationEndpoint.class;
    }

    public boolean isSingleton() {
        return true;
    }

}
