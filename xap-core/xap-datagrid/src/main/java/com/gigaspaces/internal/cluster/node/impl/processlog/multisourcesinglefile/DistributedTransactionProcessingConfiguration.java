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

package com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile;


/**
 * @author idan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class DistributedTransactionProcessingConfiguration {
    private long _timeoutBeforePartialCommit;
    private long _waitForOperationsBeforePartialCommit;
    private boolean _monitorPendingOperationsMemory = true;
    protected boolean _overriden;

    public DistributedTransactionProcessingConfiguration(
            long distributedTransactionWaitTimeout,
            long distributedTransactionWaitForOperations) {
        _timeoutBeforePartialCommit = distributedTransactionWaitTimeout;
        setWaitForOperationsBeforePartialCommit(distributedTransactionWaitForOperations);
        _overriden = false;
    }

    public void setTimeoutBeforePartialCommit(long timeoutBeforePartialCommit) {
        if (timeoutBeforePartialCommit < 0)
            throw new IllegalArgumentException("timeoutBeforePartialCommit was set to "
                    + timeoutBeforePartialCommit
                    + " but should be greater than 0");
        _timeoutBeforePartialCommit = timeoutBeforePartialCommit;
        _overriden = true;
    }

    public void setWaitForOperationsBeforePartialCommit(long waitForOperationsBeforePartialCommit) {
        if (waitForOperationsBeforePartialCommit < -1)
            throw new IllegalArgumentException("waitForOperationsBeforePartialCommit was set to "
                    + waitForOperationsBeforePartialCommit
                    + " but should be -1 or greater than 0");
        if (waitForOperationsBeforePartialCommit == -1)
            _waitForOperationsBeforePartialCommit = Long.MAX_VALUE;
        else
            _waitForOperationsBeforePartialCommit = waitForOperationsBeforePartialCommit;
        _overriden = true;
    }

    public long getTimeoutBeforePartialCommit() {
        return _timeoutBeforePartialCommit;
    }

    public long getWaitForOperationsBeforePartialCommit() {
        return _waitForOperationsBeforePartialCommit;
    }

    public boolean isOverriden() {
        return _overriden;
    }

    public boolean isMonitorPendingOperationsMemory() {
        return _monitorPendingOperationsMemory;
    }

    public void setMonitorPendingOperationsMemory(
            boolean monitorPendingOperationsMemory) {
        _monitorPendingOperationsMemory = monitorPendingOperationsMemory;
        _overriden = true;
    }

    @Override
    public String toString() {
        return "DistributedTransactionProcessingConfiguration [getTimeoutBeforePartialCommit()="
                + getTimeoutBeforePartialCommit()
                + ", getWaitForOperationsBeforePartialCommit()="
                + getWaitForOperationsBeforePartialCommit()
                + ", monitorPendingOperationsMemory="
                + _monitorPendingOperationsMemory + "]";
    }


}
