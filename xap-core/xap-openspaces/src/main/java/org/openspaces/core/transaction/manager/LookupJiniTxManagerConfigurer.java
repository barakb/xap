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


package org.openspaces.core.transaction.manager;

import org.springframework.transaction.PlatformTransactionManager;

/**
 * A simple configurer for {@link org.openspaces.core.transaction.manager.LookupJiniTransactionManager}.
 *
 * @author kimchy
 */
public class LookupJiniTxManagerConfigurer {

    final private LookupJiniTransactionManager lookupJiniTransactionManager;

    private boolean initialized = false;

    public LookupJiniTxManagerConfigurer() {
        lookupJiniTransactionManager = new LookupJiniTransactionManager();
    }

    /**
     * @see org.openspaces.core.transaction.manager.LookupJiniTransactionManager#setTransactionManagerName(String)
     */
    public LookupJiniTxManagerConfigurer transactionManagerName(String transactionManagerName) {
        lookupJiniTransactionManager.setTransactionManagerName(transactionManagerName);
        return this;
    }

    /**
     * @see org.openspaces.core.transaction.manager.LookupJiniTransactionManager#setTransactionManagerName(String)
     */
    public LookupJiniTxManagerConfigurer lookupTimeout(long lookupTimeout) {
        lookupJiniTransactionManager.setLookupTimeout(lookupTimeout);
        return this;
    }

    /**
     * @see LookupJiniTransactionManager#setDefaultTimeout(int)
     */
    public LookupJiniTxManagerConfigurer defaultTimeout(int defaultTimeout) {
        lookupJiniTransactionManager.setDefaultTimeout(defaultTimeout);
        return this;
    }

    /**
     * @see LookupJiniTransactionManager#setCommitTimeout(Long)
     */
    public LookupJiniTxManagerConfigurer commitTimeout(long commitTimeout) {
        lookupJiniTransactionManager.setCommitTimeout(commitTimeout);
        return this;
    }

    /**
     * @see LookupJiniTransactionManager#setRollbackTimeout(Long)
     */
    public LookupJiniTxManagerConfigurer rollbackTimeout(Long rollbackTimeout) {
        lookupJiniTransactionManager.setRollbackTimeout(rollbackTimeout);
        return this;
    }

    /**
     * @see LookupJiniTransactionManager#setRollbackTimeout(Long)
     */
    public LookupJiniTxManagerConfigurer leaseRenewalConfig(TransactionLeaseRenewalConfig leaseRenewalConfig) {
        lookupJiniTransactionManager.setLeaseRenewalConfig(leaseRenewalConfig);
        return this;
    }

    /**
     * @see LookupJiniTransactionManager#setGroups(String[])
     */
    public LookupJiniTxManagerConfigurer lookupGroups(String... lookupGroups) {
        lookupJiniTransactionManager.setGroups(lookupGroups);
        return this;
    }

    /**
     * @see LookupJiniTransactionManager#setLocators(String[])
     */
    public LookupJiniTxManagerConfigurer lookupLocators(String... lookupLocators) {
        lookupJiniTransactionManager.setLocators(lookupLocators);
        return this;
    }

    public PlatformTransactionManager transactionManager() throws Exception {
        if (!initialized) {
            lookupJiniTransactionManager.afterPropertiesSet();
            initialized = true;
        }
        return lookupJiniTransactionManager;
    }

    public void destroy() throws Exception {
        lookupJiniTransactionManager.destroy();
    }
}