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

import org.openspaces.core.exception.DefaultExceptionTranslator;
import org.openspaces.core.exception.ExceptionTranslator;
import org.openspaces.core.transaction.DefaultTransactionProvider;
import org.openspaces.core.transaction.TransactionProvider;
import org.openspaces.core.transaction.manager.JiniPlatformTransactionManager;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.Constants;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.util.Assert;

/**
 * @author kimchy
 */
public class GigaMapFactoryBean implements InitializingBean, DisposableBean, FactoryBean, BeanNameAware {

    /**
     * Prefix for the isolation constants defined in TransactionDefinition
     */
    public static final String PREFIX_ISOLATION = "ISOLATION_";

    /**
     * Constants instance for TransactionDefinition
     */
    private static final Constants constants = new Constants(TransactionDefinition.class);

    private IMap map;

    private TransactionProvider txProvider;

    private DefaultTransactionProvider defaultTxProvider;

    private ExceptionTranslator exTranslator;

    private PlatformTransactionManager transactionManager;

    private long defaultWaitForResponse = 0;

    private long defaultTimeToLive = Long.MAX_VALUE;

    private long defaultLockTimeToLive = 60000;

    private long defaultWaitingForLockTimeout = 10000;

    private int defaultIsolationLevel = TransactionDefinition.ISOLATION_DEFAULT;

    private String beanName;

    private DefaultGigaMap gigaMap;


    /**
     * <p>Sets the map that will be used by the created {@link GigaMap}. This is a required
     * parameter to the factory.
     *
     * @param map The map used
     */
    public void setMap(IMap map) {
        this.map = map;
    }

    /**
     * <p>Sets the transaction provider that will be used by the created {@link GigaMap}. This is an
     * optional parameter and defaults to {@link org.openspaces.core.transaction.DefaultTransactionProvider}.
     *
     * @param txProvider The transaction provider to use
     */
    public void setTxProvider(TransactionProvider txProvider) {
        this.txProvider = txProvider;
    }

    /**
     * <p>Sets the exception translator that will be used by the created {@link GigaMap}. This is an
     * optional parameter and defaults to {@link org.openspaces.core.exception.DefaultExceptionTranslator}.
     *
     * @param exTranslator The exception translator to use
     */
    public void setExTranslator(ExceptionTranslator exTranslator) {
        this.exTranslator = exTranslator;
    }

    /**
     * Sets (in milliseconds) the default wait timeout when perfoming {@link GigaMap#get(Object)} or
     * {@link GigaMap#remove(Object)}. Defaults to NO WAIT (0).
     */
    public void setDefaultWaitForResponse(long defaultWaitForResponse) {
        this.defaultWaitForResponse = defaultWaitForResponse;
    }

    /**
     * Sets the default time to live (in milliseconds) for new entries. Defaults to FOREVER.
     */
    public void setDefaultTimeToLive(long defaultTimeToLive) {
        this.defaultTimeToLive = defaultTimeToLive;
    }

    /**
     * Sets (in milliseconds) the default time to live for locks. Defaults to 60 seconds.
     */
    public void setDefaultLockTimeToLive(long defaultLockTimeToLive) {
        this.defaultLockTimeToLive = defaultLockTimeToLive;
    }

    /**
     * Sets (in milliseconds) the default time to wait for a given lock when locking. Defaults to 10
     * seconds.
     */
    public void setDefaultWaitingForLockTimeout(long defaultWaitingForLockTimeout) {
        this.defaultWaitingForLockTimeout = defaultWaitingForLockTimeout;
    }

    /**
     * Set the default isolation level by the name of the corresponding constant in
     * TransactionDefinition, e.g. "ISOLATION_DEFAULT".
     *
     * @param constantName name of the constant
     * @throws IllegalArgumentException if the supplied value is not resolvable to one of the
     *                                  <code>ISOLATION_</code> constants or is <code>null</code>
     * @see #setDefaultIsolationLevel(int)
     * @see org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT
     */
    public final void setDefaultIsolationLevelName(String constantName) throws IllegalArgumentException {
        if (constantName == null || !constantName.startsWith(PREFIX_ISOLATION)) {
            throw new IllegalArgumentException("Only isolation constants allowed");
        }
        setDefaultIsolationLevel(constants.asNumber(constantName).intValue());
    }

    /**
     * Set the default isolation level. Must be one of the isolation constants in the
     * TransactionDefinition interface. Default is ISOLATION_DEFAULT.
     *
     * @throws IllegalArgumentException if the supplied value is not one of the <code>ISOLATION_</code>
     *                                  constants
     * @see org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT
     */
    public void setDefaultIsolationLevel(int defaultIsolationLevel) {
        if (!constants.getValues(PREFIX_ISOLATION).contains(Integer.valueOf(defaultIsolationLevel))) {
            throw new IllegalArgumentException("Only values of isolation constants allowed");
        }
        this.defaultIsolationLevel = defaultIsolationLevel;
    }

    /**
     * <p>Set the transaction manager to enable transactional operations. Can be <code>null</code>
     * if transactional support is not required or the default space is used as a transactional
     * context.
     */
    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public void afterPropertiesSet() {
        Assert.notNull(this.map, "map property is required");
        if (exTranslator == null) {
            exTranslator = new DefaultExceptionTranslator();
        }
        if (txProvider == null) {
            Object transactionalContext = null;
            if (transactionManager != null && transactionManager instanceof JiniPlatformTransactionManager) {
                transactionalContext = ((JiniPlatformTransactionManager) transactionManager).getTransactionalContext();
            }
            // no transaction context is set (probably since there is no transactionManager), use the space as the transaction context
            if (transactionalContext == null) {
                transactionalContext = map.getMasterSpace();
            }
            defaultTxProvider = new DefaultTransactionProvider(transactionalContext, transactionManager);
            txProvider = defaultTxProvider;
        }
        gigaMap = new DefaultGigaMap(map, txProvider, exTranslator, defaultIsolationLevel);
        gigaMap.setDefaultTimeToLive(defaultTimeToLive);
        gigaMap.setDefaultWaitForResponse(defaultWaitForResponse);
        gigaMap.setDefaultLockTimeToLive(defaultLockTimeToLive);
        gigaMap.setDefaultWaitingForLockTimeout(defaultWaitingForLockTimeout);
    }

    /**
     * Return {@link GigaMap} implementation constructed in the {@link #afterPropertiesSet()}
     * phase.
     */
    public Object getObject() {
        return this.gigaMap;
    }

    public Class<? extends GigaMap> getObjectType() {
        return (gigaMap == null ? GigaMap.class : gigaMap.getClass());
    }

    /**
     * Returns <code>true</code> as this is a singleton.
     */
    public boolean isSingleton() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.DisposableBean#destroy()
     */
    @Override
    public void destroy() throws Exception {
        if (defaultTxProvider != null)
            defaultTxProvider.destroy();
    }
}
