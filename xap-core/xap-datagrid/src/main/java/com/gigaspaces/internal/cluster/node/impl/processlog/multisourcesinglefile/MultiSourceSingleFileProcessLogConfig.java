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

import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessLogConfig;

/**
 * @author idan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class MultiSourceSingleFileProcessLogConfig extends GlobalOrderProcessLogConfig {
    /**
     * Values are configured through {@link DistributedTransactionProcessingConfiguration}
     */
    private long _pendingPacketPacketsIntervalBeforeConsumption = Long.MAX_VALUE;
    private int _pendingPacketsQueueSizeThreshold = 1000;
    private boolean _monitorPendingOperationsMemory = true;

    private final double _highMemoryUsagePercentage = 95.0;

    /**
     * Gets the packets interval before consuming a pending packet
     */
    public long getPendingPacketPacketsIntervalBeforeConsumption() {
        return _pendingPacketPacketsIntervalBeforeConsumption;
    }

    /**
     * Gets the pending time before consuming a pending packet
     */
    public void setPendingPacketPacketsIntervalBeforeConsumption(
            long pendingPacketPacketsIntervalBeforeConsumption) {
        _pendingPacketPacketsIntervalBeforeConsumption = pendingPacketPacketsIntervalBeforeConsumption;
    }

    /**
     * @return Pending packets queue size threshold which after memory monitoring will occur.
     */
    public int getPendingPacketsQueueSizeThreshold() {
        return _pendingPacketsQueueSizeThreshold;
    }

    /**
     * Sets the pending packets queue size threshold which after memory monitoring will occur.
     */
    public void setPendingPacketsQueueSizeThreshold(int pendingPacketsQueueSizeThreshold) {
        _pendingPacketsQueueSizeThreshold = pendingPacketsQueueSizeThreshold;
    }

    /**
     * @return The memory usage percentage which after a {@link MemoryShortageException} will be
     * thrown upon batch receive.
     */
    public double getHighMemoryUsagePercentage() {
        return _highMemoryUsagePercentage;
    }

    /**
     * Sets whether to enable pending packets queue memory monitoring.
     */
    public void setMonitorPendingOperationsMemory(boolean memoryMonitoringEnabled) {
        _monitorPendingOperationsMemory = memoryMonitoringEnabled;
    }

    /**
     * @return Whether pending packets queue memory monitoring is enabled.
     */
    public boolean isMonitorPendingOperationsMemory() {
        return _monitorPendingOperationsMemory;
    }

    @Override
    public String toString() {
        return "MultiSourceSingleFileProcessLogConfig [pendingPacketPacketsIntervalBeforeConsumption="
                + _pendingPacketPacketsIntervalBeforeConsumption
                + ", pendingPacketsQueueSizeThreshold="
                + _pendingPacketsQueueSizeThreshold
                + ", monitorPendingOperationsMemory="
                + _monitorPendingOperationsMemory
                + ", highMemoryUsagePercentage="
                + _highMemoryUsagePercentage
                + "]";
    }

}
