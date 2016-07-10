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
import com.j_spaces.core.IJSpace;

import org.openspaces.core.exception.ExceptionTranslator;
import org.openspaces.core.space.SpaceConfigurer;
import org.openspaces.core.transaction.TransactionProvider;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A simple programmatic configurer for {@link org.openspaces.core.GigaSpace} instance wrapping the
 * {@link org.openspaces.core.GigaSpaceFactoryBean}.
 *
 * <p>Usage example:
 * <pre>
 * UrlSpaceConfigurer urlSpaceConfigurer = new UrlSpaceConfigurer("/./space").schema("persistent")
 *          .noWriteLeaseMode(true).lookupGroups(new String[] {"kimchy"});
 *
 * GigaSpace gigaSpace = new GigaSpaceConfigurer(urlSpaceConfigurer).defaultTakeTimeout(1000).gigaSpace();
 * ...
 * urlSpaceConfigurer.destroy(); // optional
 * </pre>
 *
 * @author kimchy
 */
public class GigaSpaceConfigurer {

    private final GigaSpaceFactoryBean gigaSpaceFactoryBean;

    private GigaSpace gigaSpace;

    /**
     * Constructs a new configurer based on the Space.
     */
    public GigaSpaceConfigurer(IJSpace space) {
        gigaSpaceFactoryBean = new GigaSpaceFactoryBean();
        gigaSpaceFactoryBean.setSpace(space);
    }

    /**
     * Constructs a new configurer based on the Space.
     */
    public GigaSpaceConfigurer(SpaceConfigurer configurer) {
        gigaSpaceFactoryBean = new GigaSpaceFactoryBean();
        gigaSpaceFactoryBean.setSpace(configurer.space());
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setTxProvider(org.openspaces.core.transaction.TransactionProvider)
     */
    public GigaSpaceConfigurer txProvider(TransactionProvider txProvider) {
        gigaSpaceFactoryBean.setTxProvider(txProvider);
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setExTranslator(org.openspaces.core.exception.ExceptionTranslator)
     */
    public GigaSpaceConfigurer exTranslator(ExceptionTranslator exTranslator) {
        gigaSpaceFactoryBean.setExTranslator(exTranslator);
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setClustered(boolean)
     */
    public GigaSpaceConfigurer clustered(boolean clustered) {
        gigaSpaceFactoryBean.setClustered(clustered);
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultReadTimeout(long)
     */
    public GigaSpaceConfigurer defaultReadTimeout(long defaultReadTimeout) {
        gigaSpaceFactoryBean.setDefaultReadTimeout(defaultReadTimeout);
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultTakeTimeout(long)
     */
    public GigaSpaceConfigurer defaultTakeTimeout(long defaultTakeTimeout) {
        gigaSpaceFactoryBean.setDefaultTakeTimeout(defaultTakeTimeout);
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultWriteLease(long)
     */
    public GigaSpaceConfigurer defaultWriteLease(long defaultWriteLease) {
        gigaSpaceFactoryBean.setDefaultWriteLease(defaultWriteLease);
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultIsolationLevel(int)
     */
    public GigaSpaceConfigurer defaultIsolationLevel(int defaultIsolationLevel) {
        gigaSpaceFactoryBean.setDefaultIsolationLevel(defaultIsolationLevel);
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultWriteModifiers(com.gigaspaces.client.WriteModifiers[])
     */
    public GigaSpaceConfigurer defaultWriteModifiers(WriteModifiers defaultWriteModifiers) {
        gigaSpaceFactoryBean.setDefaultWriteModifiers(new WriteModifiers[]{defaultWriteModifiers});
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultReadModifiers(com.gigaspaces.client.ReadModifiers[])
     */
    public GigaSpaceConfigurer defaultReadModifiers(ReadModifiers defaultReadModifiers) {
        gigaSpaceFactoryBean.setDefaultReadModifiers(new ReadModifiers[]{defaultReadModifiers});
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultTakeModifiers(com.gigaspaces.client.TakeModifiers[])
     */
    public GigaSpaceConfigurer defaultTakeModifiers(TakeModifiers defaultTakeModifiers) {
        gigaSpaceFactoryBean.setDefaultTakeModifiers(new TakeModifiers[]{defaultTakeModifiers});
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultCountModifiers(com.gigaspaces.client.CountModifiers[])
     */
    public GigaSpaceConfigurer defaultCountModifiers(CountModifiers defaultCountModifiers) {
        gigaSpaceFactoryBean.setDefaultCountModifiers(new CountModifiers[]{defaultCountModifiers});
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultClearModifiers(com.gigaspaces.client.ClearModifiers[])
     */
    public GigaSpaceConfigurer defaultClearModifiers(ClearModifiers defaultClearModifiers) {
        gigaSpaceFactoryBean.setDefaultClearModifiers(new ClearModifiers[]{defaultClearModifiers});
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setDefaultChangeModifiers(com.gigaspaces.client.ChangeModifiers[])
     */
    public GigaSpaceConfigurer defaultChangeModifiers(ChangeModifiers defaultChangeModifiers) {
        gigaSpaceFactoryBean.setDefaultChangeModifiers(new ChangeModifiers[]{defaultChangeModifiers});
        return this;
    }

    /**
     * @see org.openspaces.core.GigaSpaceFactoryBean#setTransactionManager(org.springframework.transaction.PlatformTransactionManager)
     */
    public GigaSpaceConfigurer transactionManager(PlatformTransactionManager transactionManager) {
        gigaSpaceFactoryBean.setTransactionManager(transactionManager);
        return this;
    }

    /**
     * Creates a new {@link org.openspaces.core.GigaSpace} instance if non already created.
     */
    public GigaSpace create() {
        if (gigaSpace == null) {
            gigaSpaceFactoryBean.afterPropertiesSet();
            gigaSpace = (GigaSpace) gigaSpaceFactoryBean.getObject();
        }
        return gigaSpace;
    }

    /**
     * Creates a new {@link org.openspaces.core.GigaSpace} instance if non already created.
     *
     * @see #create()
     */
    public GigaSpace gigaSpace() {
        return create();
    }
}
