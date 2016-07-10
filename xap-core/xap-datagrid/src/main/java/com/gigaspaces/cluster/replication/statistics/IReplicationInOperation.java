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


package com.gigaspaces.cluster.replication.statistics;

/**
 * Represents specific incoming operation statistics.
 *
 * In a cluster that is active the numbers should be used as an estimation only, since statistics
 * gathering is concurrent. In a cluster that doesn't have any activity the numbers are accurate.
 *
 * The statistics should comply to the following formula :
 *
 * operationCount = successfulCount + failedCount + discardedCount + inProgressCount.
 *
 * @author anna
 * @since 8.0
 */
public interface IReplicationInOperation {

    /**
     * Returns the total count of operations that were executed on target
     */
    public long getOperationCount();

    /**
     * Returns the count of operations that were successfully executed on target
     */
    public long getSuccessfulOperationCount();

    /**
     * Returns the count of operations that failed on target
     */
    public long getFailedOperationCount();

    /**
     * Returns the count of operations that were discarded by the target.
     */
    public long getDiscardedOperationCount();

    /**
     * Returns the count of operations that are currently in progress on the target
     */
    public long getInProgressOperationCount();

}