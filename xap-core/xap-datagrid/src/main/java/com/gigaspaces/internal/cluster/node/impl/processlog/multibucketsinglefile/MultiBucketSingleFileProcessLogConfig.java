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

package com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogConfig;

@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileProcessLogConfig
        extends ProcessLogConfig {

    private short _bucketsCount = 4; //Do not change default, will ruin unit tests
    private int _batchProcessingThreshold = Integer.MAX_VALUE;
    private int _batchParallelProcessingThreshold = 50;
    private int _batchParallelFactor = Runtime.getRuntime().availableProcessors();

    public void setBucketsCount(short bucketsCount) {
        _bucketsCount = bucketsCount;
    }

    public short getBucketsCount() {
        return _bucketsCount;
    }

    public void setBatchProcessingThreshold(int batchProcessingThreshold) {
        _batchProcessingThreshold = batchProcessingThreshold;
    }

    public int getBatchProcessingThreshold() {
        return _batchProcessingThreshold;
    }

    public void setBatchParallelProcessingThreshold(
            int batchParallelProcessingThreshold) {
        _batchParallelProcessingThreshold = batchParallelProcessingThreshold;
    }

    public int getBatchParallelProcessingThreshold() {
        return _batchParallelProcessingThreshold;
    }

    public int getBatchParallelFactor() {
        return _batchParallelFactor;
    }

    public void setBatchParallelFactor(int batchParallelFactor) {
        _batchParallelFactor = batchParallelFactor;
    }

    @Override
    public String toString() {
        return "MultiBucketSingleFileProcessLogConfig [_bucketsCount="
                + _bucketsCount + ", _batchProcessingThreshold="
                + _batchProcessingThreshold
                + ", _batchParallelProcessingThreshold="
                + _batchParallelProcessingThreshold + ", _batchParallelFactor="
                + _batchParallelFactor + ", _consumeTimeout=" + getConsumeTimeout()
                + "]";
    }


}
