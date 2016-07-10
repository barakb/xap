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

package org.openspaces.core.transaction;


/**
 * A bean for configuring distributed transaction processing at Mirror component.
 *
 * Its possible to configure two parameters: <ul> <li> {@link #setDistributedTransactionWaitTimeout(Long)}
 * - determines the wait timeout for all distributed transaction participants data before committing
 * only the data that arrived. </li> <li> {@link #setDistributedTransactionWaitForOperations(Long)}
 * - determines the number of operations to wait for before committing a distributed transaction
 * when data from all participants haven't arrived. </li> </ul>
 *
 * @author idan
 * @since 8.0.4
 */
public class DistributedTransactionProcessingConfigurationFactoryBean {

    private Long distributedTransactionWaitTimeout;
    private Long distributedTransactionWaitForOperations;
    private Boolean monitorPendingOperationsMemory;

    public DistributedTransactionProcessingConfigurationFactoryBean() {
    }

    public Long getDistributedTransactionWaitTimeout() {
        return distributedTransactionWaitTimeout;
    }

    public void setDistributedTransactionWaitTimeout(Long distributedTransactionWaitTimeout) {
        this.distributedTransactionWaitTimeout = distributedTransactionWaitTimeout;
    }

    public Long getDistributedTransactionWaitForOperations() {
        return distributedTransactionWaitForOperations;
    }

    public void setDistributedTransactionWaitForOperations(Long distributedTransactionWaitForOperations) {
        this.distributedTransactionWaitForOperations = distributedTransactionWaitForOperations;
    }

    public Boolean isMonitorPendingOperationsMemory() {
        return monitorPendingOperationsMemory;
    }

    public void setMonitorPendingOperationsMemory(Boolean monitorPendingOperationsMemory) {
        this.monitorPendingOperationsMemory = monitorPendingOperationsMemory;
    }

}
