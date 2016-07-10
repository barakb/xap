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

/**
 *
 */
package com.gigaspaces.cluster.replication.async.mirror;

/**
 * Abstract class for {@link MirrorOperations} implementations. Implements the operation statistics
 * aggregation.
 *
 * @author anna
 * @since 7.1.1
 */
public abstract class AbstractMirrorOperations
        implements MirrorOperations {

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("  MirrorOperationsImpl\n    [\n        getOperationCount()=");
        builder.append(getOperationCount());
        builder.append(", \n        getSuccessfulOperationCount()=");
        builder.append(getSuccessfulOperationCount());
        builder.append(", \n        getFailedOperationCount()=");
        builder.append(getFailedOperationCount());
        builder.append(", \n        getDiscardedOperationCount()=");
        builder.append(getDiscardedOperationCount());
        builder.append(", \n        getRemoveOperationStatistics()=");
        builder.append(getRemoveOperationStatistics());
        builder.append(", \n        getUpdateOperationStatistics()=");
        builder.append(getUpdateOperationStatistics());
        builder.append(", \n        getWriteOperationStatistics()=");
        builder.append(getWriteOperationStatistics());
        builder.append(", \n        getChangeOperationStatistics()=");
        builder.append(getChangeOperationStatistics());
        builder.append("\n    ]");
        return builder.toString();
    }

    public long getDiscardedOperationCount() {
        return getWriteOperationStatistics().getDiscardedOperationCount()
                + getUpdateOperationStatistics().getDiscardedOperationCount()
                + getRemoveOperationStatistics().getDiscardedOperationCount()
                + getChangeOperationStatistics().getDiscardedOperationCount();

    }

    public long getFailedOperationCount() {
        return getWriteOperationStatistics().getFailedOperationCount()
                + getUpdateOperationStatistics().getFailedOperationCount()
                + getRemoveOperationStatistics().getFailedOperationCount()
                + getChangeOperationStatistics().getFailedOperationCount();

    }

    public long getOperationCount() {
        return getWriteOperationStatistics().getOperationCount()
                + getUpdateOperationStatistics().getOperationCount()
                + getRemoveOperationStatistics().getOperationCount()
                + getChangeOperationStatistics().getOperationCount();

    }

    public long getSuccessfulOperationCount() {
        return getWriteOperationStatistics().getSuccessfulOperationCount()
                + getUpdateOperationStatistics().getSuccessfulOperationCount()
                + getRemoveOperationStatistics().getSuccessfulOperationCount()
                + getChangeOperationStatistics().getSuccessfulOperationCount();

    }

    public long getInProgressOperationCount() {
        long completedOperationCount = getSuccessfulOperationCount()
                + getFailedOperationCount() + getDiscardedOperationCount();
        return getOperationCount() - completedOperationCount;
    }

}
