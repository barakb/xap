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

import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ClearModifiers;
import com.gigaspaces.client.CountModifiers;
import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.client.TakeModifiers;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.internal.client.cache.ISpaceCache;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.IJSpace;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.exception.DefaultExceptionTranslator;
import org.openspaces.core.exception.ExceptionTranslator;
import org.openspaces.core.transaction.DefaultTransactionProvider;
import org.openspaces.core.transaction.TransactionProvider;
import org.openspaces.core.transaction.manager.JiniPlatformTransactionManager;
import org.openspaces.core.util.SpaceUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.Constants;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.util.Assert;

/**
 * <p>A factory bean creating {@link org.openspaces.core.GigaSpace GigaSpace} implementation. The
 * implementation created is {@link org.openspaces.core.DefaultGigaSpace DefaultGigaSpace} which
 * allows for pluggable {@link com.j_spaces.core.IJSpace IJSpace}, {@link
 * org.openspaces.core.transaction.TransactionProvider TransactionProvider}, and {@link
 * org.openspaces.core.exception.ExceptionTranslator ExceptionTranslator}.
 *
 * <p>The factory requires an {@link com.j_spaces.core.IJSpace IJSpace} which can be either directly
 * acquired or build using one of the several space factory beans provided in
 * <code>org.openspaces.core.space</code>.
 *
 * <p>The factory accepts an optional {@link org.openspaces.core.transaction.TransactionProvider
 * TransactionProvider} which defaults to {@link org.openspaces.core.transaction.DefaultTransactionProvider
 * DefaultTransactionProvider}. The transactional context used is based on {@link
 * #setTransactionManager(org.springframework.transaction.PlatformTransactionManager)} and if no
 * transaction manager is provided, will use the space as the context.
 *
 * <p>When using {@link org.openspaces.core.transaction.manager.LocalJiniTransactionManager} there
 * is no need to pass the transaction manager to this factory, since both by default will use the
 * space as the transactional context. When working with {@link org.openspaces.core.transaction.manager.LookupJiniTransactionManager}
 * (which probably means Mahalo and support for more than one space as transaction resources) the
 * transaction manager should be provided to this class.
 *
 * <p>The factory accepts an optional {@link org.openspaces.core.exception.ExceptionTranslator
 * ExceptionTranslator} which defaults to {@link org.openspaces.core.exception.DefaultExceptionTranslator
 * DefaultExceptionTranslator}.
 *
 * <p>A clustered flag allows to control if this GigaSpace instance will work against a clustered
 * view of the space or directly against a clustered member. This flag has no affect when not
 * working in a clustered mode (partitioned or primary/backup). By default if this flag is not set
 * it will be set automatically by this factory. It will be set to <code>true</code> if the space is
 * an embedded one AND the space is not a local cache proxy. It will be set to <code>false</code>
 * otherwise (i.e. the space is not an embedded space OR the space is a local cache proxy). A local
 * cache proxy is an <code>IJSpace</code> that is injected using {@link
 * #setSpace(com.j_spaces.core.IJSpace)} and was created using either {@link
 * org.openspaces.core.space.cache.LocalViewSpaceFactoryBean} or {@link
 * org.openspaces.core.space.cache.LocalCacheSpaceFactoryBean}.
 *
 * <p>The factory allows to set the default read/take timeout and write lease when using the same
 * operations without the relevant parameters.
 *
 * <p>The factory also allows to set the default isolation level for read operations that will be
 * performed by {@link org.openspaces.core.GigaSpace} API. The isolation level can be set either
 * using {@link #setDefaultIsolationLevel(int)} or {@link #setDefaultIsolationLevelName(String)}.
 * Note, this setting will apply when not working under Spring declarative transactions or when
 * using Spring declarative transaction with the default isolation level ({@link
 * org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT}).
 *
 * @author kimchy
 * @see org.openspaces.core.GigaSpace
 * @see org.openspaces.core.DefaultGigaSpace
 * @see org.openspaces.core.transaction.TransactionProvider
 * @see org.openspaces.core.exception.ExceptionTranslator
 * @see org.openspaces.core.transaction.manager.AbstractJiniTransactionManager
 */
public class GigaSpaceFactoryBean implements InitializingBean, DisposableBean, FactoryBean, BeanNameAware {

    private static final Log logger = LogFactory.getLog(GigaSpaceFactoryBean.class);

    /**
     * Prefix for the isolation constants defined in TransactionDefinition
     */
    public static final String PREFIX_ISOLATION = "ISOLATION_";

    /**
     * Constants instance for TransactionDefinition
     */
    private static final Constants constants = new Constants(TransactionDefinition.class);

    private ISpaceProxy space;

    private TransactionProvider txProvider;

    private DefaultTransactionProvider defaultTxProvider;

    private ExceptionTranslator exTranslator;

    private PlatformTransactionManager transactionManager;

    private Boolean clustered;

    private long defaultReadTimeout = 0;

    private long defaultTakeTimeout = 0;

    private long defaultWriteLease = Long.MAX_VALUE;

    private int defaultIsolationLevel = TransactionDefinition.ISOLATION_DEFAULT;

    private WriteModifiers[] defaultWriteModifiers;

    private ReadModifiers[] defaultReadModifiers;

    private TakeModifiers[] defaultTakeModifiers;

    private ClearModifiers[] defaultClearModifiers;

    private CountModifiers[] defaultCountModifiers;

    private ChangeModifiers[] defaultChangeModifiers;

    private String beanName;

    private DefaultGigaSpace gigaSpace;

    /**
     * <p>Sets the space that will be used by the created {@link org.openspaces.core.GigaSpace}.
     * This is a required parameter to the factory.
     *
     * @param space The space used
     */
    public void setSpace(IJSpace space) {
        this.space = (ISpaceProxy) space;
    }

    /**
     * <p>Sets the transaction provider that will be used by the created {@link
     * org.openspaces.core.GigaSpace}. This is an optional parameter and defaults to {@link
     * org.openspaces.core.transaction.DefaultTransactionProvider}.
     *
     * @param txProvider The transaction provider to use
     */
    public void setTxProvider(TransactionProvider txProvider) {
        this.txProvider = txProvider;
    }

    /**
     * <p>Sets the exception translator that will be used by the created {@link
     * org.openspaces.core.GigaSpace}. This is an optional parameter and defaults to {@link
     * org.openspaces.core.exception.DefaultExceptionTranslator}.
     *
     * @param exTranslator The exception translator to use
     */
    public void setExTranslator(ExceptionTranslator exTranslator) {
        this.exTranslator = exTranslator;
    }

    /**
     * <p>Sets the cluster flag controlling if this {@link org.openspaces.core.GigaSpace} will work
     * with a clustered view of the space or directly with a cluster member. By default if this flag
     * is not set it will be set automatically by this factory. It will be set to <code>false</code>
     * if the space is an embedded one AND the space is not a local cache proxy. It will be set to
     * <code>true</code> otherwise (i.e. the space is not an embedded space OR the space is a local
     * cache proxy).
     *
     * @param clustered If the {@link org.openspaces.core.GigaSpace} is going to work with a
     *                  clustered view of the space or directly with a cluster member
     */
    public void setClustered(boolean clustered) {
        this.clustered = clustered;
    }

    /**
     * <p>Sets the default read timeout for {@link org.openspaces.core.GigaSpace#read(Object)} and
     * {@link org.openspaces.core.GigaSpace#readIfExists(Object)} operations. Default to 0.
     */
    public void setDefaultReadTimeout(long defaultReadTimeout) {
        this.defaultReadTimeout = defaultReadTimeout;
    }

    /**
     * <p>Sets the default take timeout for {@link org.openspaces.core.GigaSpace#take(Object)} and
     * {@link org.openspaces.core.GigaSpace#takeIfExists(Object)} operations. Default to 0.
     */
    public void setDefaultTakeTimeout(long defaultTakeTimeout) {
        this.defaultTakeTimeout = defaultTakeTimeout;
    }

    /**
     * <p>Sets the default write lease for {@link org.openspaces.core.GigaSpace#write(Object)}
     * operation. Default to {@link net.jini.core.lease.Lease#FOREVER}.
     */
    public void setDefaultWriteLease(long defaultWriteLease) {
        this.defaultWriteLease = defaultWriteLease;
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
     * Set the default {@link WriteModifiers} to be used for write operations on the {@link
     * GigaSpace} instance. Defaults to {@link WriteModifiers#UPDATE_OR_WRITE}
     *
     * @param defaultWriteModifiers The default write modifiers.
     * @see WriteModifiers
     */
    public void setDefaultWriteModifiers(WriteModifiers[] defaultWriteModifiers) {
        this.defaultWriteModifiers = defaultWriteModifiers;
    }

    /**
     * Set the default {@link ReadModifiers} to be used for read operations on the {@link GigaSpace}
     * instance. Defaults to {@link ReadModifiers#READ_COMMITTED}
     *
     * @param defaultReadModifiers The default read modifiers.
     * @see ReadModifiers
     */
    public void setDefaultReadModifiers(ReadModifiers[] defaultReadModifiers) {
        this.defaultReadModifiers = defaultReadModifiers;
    }

    /**
     * Set the default {@link TakeModifiers} to be used for take operations on the {@link GigaSpace}
     * instance. Defaults to {@link TakeModifiers#NONE}
     *
     * @param defaultTakeModifiers The default take modifiers.
     * @see TakeModifiers
     */
    public void setDefaultTakeModifiers(TakeModifiers[] defaultTakeModifiers) {
        this.defaultTakeModifiers = defaultTakeModifiers;
    }

    /**
     * Set the default {@link CountModifiers} to be used for count operations on the {@link
     * GigaSpace} instance. Defaults to {@link CountModifiers#NONE}
     *
     * @param defaultCountModifiers The default count modifiers.
     * @see CountModifiers
     */
    public void setDefaultCountModifiers(CountModifiers[] defaultCountModifiers) {
        this.defaultCountModifiers = defaultCountModifiers;
    }

    /**
     * Set the default {@link ClearModifiers} to be used for clear operations on the {@link
     * GigaSpace} instance. Defaults to {@link ClearModifiers#NONE}
     *
     * @param defaultClearModifiers The default clear modifiers.
     * @see ClearModifiers
     */
    public void setDefaultClearModifiers(ClearModifiers[] defaultClearModifiers) {
        this.defaultClearModifiers = defaultClearModifiers;
    }

    /**
     * Set the default {@link ChangeModifiers} to be used for change operations on the {@link
     * GigaSpace} instance. Defaults to {@link ChangeModifiers#NONE}
     *
     * @param defaultChangeModifiers The default change modifiers.
     * @see ChangeModifiers
     */
    public void setDefaultChangeModifiers(ChangeModifiers[] defaultChangeModifiers) {
        this.defaultChangeModifiers = defaultChangeModifiers;
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

    /**
     * Constructs the {@link org.openspaces.core.GigaSpace} instance using the {@link
     * org.openspaces.core.DefaultGigaSpace} implementation. Uses the clustered flag to get a
     * cluster member directly (if set to <code>false</code>) and applies the different defaults).
     */
    public void afterPropertiesSet() {
        Assert.notNull(this.space, "space property is required");
        ISpaceProxy space = this.space;
        if (clustered == null) {
            // in case the space is a local cache space, set the clustered flag to true since we do
            // not want to get the actual member (the cluster flag was set on the local cache already)
            if (space instanceof ISpaceCache) {
                clustered = true;
                if (logger.isDebugEnabled()) {
                    logger.debug("Clustered flag automatically set to [" + clustered + "] since the space is a local cache space for bean [" + beanName + "]");
                }
            } else {
                clustered = SpaceUtils.isRemoteProtocol(space);
                if (logger.isDebugEnabled()) {
                    logger.debug("Clustered flag automatically set to [" + clustered + "] for bean [" + beanName + "]");
                }
            }
        }
        if (!clustered && space.isClustered()) {
            space = (ISpaceProxy) SpaceUtils.getClusterMemberSpace(space);
        }
        if (exTranslator == null) {
            exTranslator = new DefaultExceptionTranslator();
        }
        if (txProvider == null) {
            Object transactionalContext = null;
            if (transactionManager != null && transactionManager instanceof JiniPlatformTransactionManager) {
                transactionalContext = ((JiniPlatformTransactionManager) transactionManager).getTransactionalContext();
            }
            defaultTxProvider = new DefaultTransactionProvider(transactionalContext, transactionManager);
            txProvider = defaultTxProvider;
        }
        gigaSpace = new DefaultGigaSpace(space, txProvider, exTranslator, defaultIsolationLevel);
        gigaSpace.setDefaultReadTimeout(defaultReadTimeout);
        gigaSpace.setDefaultTakeTimeout(defaultTakeTimeout);
        gigaSpace.setDefaultWriteLease(defaultWriteLease);
        setDefaultModifiers();

        gigaSpace.setName(beanName == null ? space.getName() : beanName);

    }

    private void setDefaultModifiers() {
        if (defaultWriteModifiers != null) {
            gigaSpace.setDefaultWriteModifiers(new WriteModifiers(defaultWriteModifiers));
        }
        if (defaultReadModifiers != null) {
            gigaSpace.setDefaultReadModifiers(new ReadModifiers(defaultReadModifiers));
        }
        if (defaultTakeModifiers != null) {
            gigaSpace.setDefaultTakeModifiers(new TakeModifiers(defaultTakeModifiers));
        }
        if (defaultCountModifiers != null) {
            gigaSpace.setDefaultCountModifiers(new CountModifiers(defaultCountModifiers));
        }
        if (defaultClearModifiers != null) {
            gigaSpace.setDefaultClearModifiers(new ClearModifiers(defaultClearModifiers));
        }
        if (defaultChangeModifiers != null) {
            gigaSpace.setDefaultChangeModifiers(new ChangeModifiers(defaultChangeModifiers));
        }
    }

    /**
     * Return {@link org.openspaces.core.GigaSpace} implementation constructed in the {@link
     * #afterPropertiesSet()} phase.
     */
    public Object getObject() {
        return this.gigaSpace;
    }

    public Class<? extends GigaSpace> getObjectType() {
        return (gigaSpace == null ? GigaSpace.class : gigaSpace.getClass());
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
