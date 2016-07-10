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


package org.openspaces.core;

import com.j_spaces.map.IMap;

import org.openspaces.core.exception.ExceptionTranslator;
import org.openspaces.core.transaction.TransactionProvider;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A simple programmatic configurer for {@link GigaMap} instance wrapping the {@link
 * GigaMapFactoryBean}.
 *
 * <p>Usage example:
 * <pre>
 * UrlSpaceConfigurer urlSpaceConfigurer = new UrlSpaceConfigurer("/./space").schema("persistent")
 *          .noWriteLeaseMode(true).lookupGroups(new String[] {"kimchy"});
 * IJSpace space = urlSpaceConfigurer.space();
 * IMap map = new MapConfigurer(space).localCachePutFirst(true).map();
 *
 * GigaMap gigaMap = new GigaMapConfigurer(map).gigaMap();
 * ...
 * urlSpaceConfigurer.destroySpace(); // optional
 * </pre>
 *
 * @author kimchy
 */
public class GigaMapConfigurer {

    final private GigaMapFactoryBean gigaMapFactoryBean;

    private GigaMap gigaMap;

    public GigaMapConfigurer(IMap map) {
        gigaMapFactoryBean = new GigaMapFactoryBean();
        gigaMapFactoryBean.setMap(map);
    }

    /**
     * @see GigaMapFactoryBean#setTxProvider(org.openspaces.core.transaction.TransactionProvider)
     */
    public GigaMapConfigurer txProvider(TransactionProvider txProvider) {
        gigaMapFactoryBean.setTxProvider(txProvider);
        return this;
    }

    /**
     * @see GigaMapFactoryBean#setExTranslator(org.openspaces.core.exception.ExceptionTranslator)
     */
    public GigaMapConfigurer exTranslator(ExceptionTranslator exTranslator) {
        gigaMapFactoryBean.setExTranslator(exTranslator);
        return this;
    }

    /**
     * @see GigaMapFactoryBean#setDefaultWaitForResponse(long)
     */
    public GigaMapConfigurer setDefaultWaitForResponse(long defaultWaitForResponse) {
        gigaMapFactoryBean.setDefaultWaitForResponse(defaultWaitForResponse);
        return this;
    }

    /**
     * @see GigaMapFactoryBean#setDefaultTimeToLive(long)
     */
    public GigaMapConfigurer defaultTimeToLive(long defaultTimeToLive) {
        gigaMapFactoryBean.setDefaultTimeToLive(defaultTimeToLive);
        return this;
    }

    /**
     * @see GigaMapFactoryBean#setDefaultLockTimeToLive(long)
     */
    public GigaMapConfigurer defaultLockTimeToLive(long defaultLockTimeToLive) {
        gigaMapFactoryBean.setDefaultLockTimeToLive(defaultLockTimeToLive);
        return this;
    }

    /**
     * @see GigaMapFactoryBean#setDefaultWaitingForLockTimeout(long)
     */
    public GigaMapConfigurer defaultWaitingForLockTimeout(long defaultWaitingForLockTimeout) {
        gigaMapFactoryBean.setDefaultWaitingForLockTimeout(defaultWaitingForLockTimeout);
        return this;
    }

    /**
     * @see GigaMapFactoryBean#setDefaultIsolationLevel(int)
     */
    public GigaMapConfigurer defaultIsolationLevel(int defaultIsolationLevel) {
        gigaMapFactoryBean.setDefaultIsolationLevel(defaultIsolationLevel);
        return this;
    }

    /**
     * @see GigaMapFactoryBean#setTransactionManager(org.springframework.transaction.PlatformTransactionManager)
     */
    public GigaMapConfigurer transactionManager(PlatformTransactionManager transactionManager) {
        gigaMapFactoryBean.setTransactionManager(transactionManager);
        return this;
    }

    /**
     * Creates a new {@link GigaMap} instance if non already created.
     */
    public GigaMap gigaMap() {
        if (gigaMap == null) {
            gigaMapFactoryBean.afterPropertiesSet();
            gigaMap = (GigaMap) gigaMapFactoryBean.getObject();
        }
        return gigaMap;
    }
}
