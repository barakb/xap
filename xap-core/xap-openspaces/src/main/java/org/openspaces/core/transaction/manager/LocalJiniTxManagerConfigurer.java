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

import com.j_spaces.core.IJSpace;

import org.openspaces.core.space.UrlSpaceConfigurer;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A simple configurer
 *
 * @author kimchy
 * @deprecated since 8.0 - use {@link DistributedJiniTxManagerConfigurer} instead.
 */
@Deprecated
public class LocalJiniTxManagerConfigurer {

    final private DistributedJiniTxManagerConfigurer distConfigurer;

    public LocalJiniTxManagerConfigurer(UrlSpaceConfigurer urlSpaceConfigurer) {
        this(urlSpaceConfigurer.space());
    }

    public LocalJiniTxManagerConfigurer(IJSpace space) {
        distConfigurer = new DistributedJiniTxManagerConfigurer();
    }


    public LocalJiniTxManagerConfigurer clustered(boolean clustered) {
        return this;
    }


    public LocalJiniTxManagerConfigurer defaultTimeout(int defaultTimeout) {
        distConfigurer.defaultTimeout(defaultTimeout);
        return this;
    }


    public LocalJiniTxManagerConfigurer commitTimeout(long commitTimeout) {
        distConfigurer.commitTimeout(commitTimeout);
        return this;
    }


    public LocalJiniTxManagerConfigurer rollbackTimeout(Long rollbackTimeout) {
        distConfigurer.rollbackTimeout(rollbackTimeout);
        return this;
    }


    public LocalJiniTxManagerConfigurer leaseRenewalConfig(TransactionLeaseRenewalConfig leaseRenewalConfig) {
        distConfigurer.leaseRenewalConfig(leaseRenewalConfig);
        return this;
    }

    public PlatformTransactionManager transactionManager() throws Exception {
        return distConfigurer.transactionManager();
    }

    public void destroy() throws Exception {
        distConfigurer.destroy();
    }
}