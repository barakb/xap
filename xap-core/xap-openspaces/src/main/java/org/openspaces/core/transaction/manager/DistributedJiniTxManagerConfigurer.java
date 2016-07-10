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
 * A simple configurer for {@link DistributedJiniTransactionManager}.
 *
 * @author kimchy
 */
public class DistributedJiniTxManagerConfigurer {

    private DistributedJiniTransactionManager distributedJiniTransactionManager;

    private boolean initialized = false;

    public DistributedJiniTxManagerConfigurer() {
        distributedJiniTransactionManager = new DistributedJiniTransactionManager();
    }

    /**
     * @see DistributedJiniTransactionManager#setDefaultTimeout(int)
     */
    public DistributedJiniTxManagerConfigurer defaultTimeout(int defaultTimeout) {
        distributedJiniTransactionManager.setDefaultTimeout(defaultTimeout);
        return this;
    }

    /**
     * @see DistributedJiniTransactionManager#setCommitTimeout(Long)
     */
    public DistributedJiniTxManagerConfigurer commitTimeout(long commitTimeout) {
        distributedJiniTransactionManager.setCommitTimeout(commitTimeout);
        return this;
    }

    /**
     * @see DistributedJiniTransactionManager#setRollbackTimeout(Long)
     */
    public DistributedJiniTxManagerConfigurer rollbackTimeout(Long rollbackTimeout) {
        distributedJiniTransactionManager.setRollbackTimeout(rollbackTimeout);
        return this;
    }

    /**
     * @see DistributedJiniTransactionManager#setRollbackTimeout(Long)
     */
    public DistributedJiniTxManagerConfigurer leaseRenewalConfig(TransactionLeaseRenewalConfig leaseRenewalConfig) {
        distributedJiniTransactionManager.setLeaseRenewalConfig(leaseRenewalConfig);
        return this;
    }

    public PlatformTransactionManager transactionManager() throws Exception {
        if (!initialized) {
            distributedJiniTransactionManager.afterPropertiesSet();
            initialized = true;
        }
        return distributedJiniTransactionManager;
    }

    public void destroy() throws Exception {
        distributedJiniTransactionManager.destroy();
    }
}