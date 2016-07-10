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


package com.gigaspaces.cluster.replication.async.mirror;

import com.gigaspaces.datasource.BulkItem;
import com.j_spaces.core.filters.ReplicationStatistics;

import java.util.Map;


/**
 * <p> Interface for mirror operation statistics. All mirror operations are executed in bulks.<br>
 * Each bulk can contain several operations that can be one of the following: WRITE,REMOVE,UPDATE.
 * See {@link BulkItem}. <p>
 *
 * MirrorStatistics shows the total operation statistics, specific replication channel statistics
 * and also statistics per operation - WRITE/UPDATE/REMOVE.
 *
 *
 * Example: <code> SpaceInstanceStatistics statistics = spaceInstance.getStatistics();
 *
 * MirrorStatistics mirrorStat =statistics.getMirrorStatistics();
 *
 * long totalOperationsCount = mirrorStat.getOperationCount(); long totalSuccessfulOperationsCount =
 * mirrorStat.getSuccessfulOperationCount(); long totalOperationsCount =
 * mirrorStat.getFailedOperationCount(); long totalWriteCount = mirrorStat.getWriteOperationStatistics().getOperationCount();
 *
 * </code>
 *
 * <p> Mirror Channels statistics shows only operations that belong to specific space. This might be
 * useful when monitoring a connection between specific space and the mirror. <p>
 *
 * To get information about what is sent to the mirror by the cluster spaces, use the {@link
 * ReplicationStatistics} API. <p>
 *
 * @author anna
 * @since 7.1.1
 */
public interface MirrorStatistics
        extends MirrorOperations {
    /**
     * Get the mirror statistics per specific space instance replication channel that replicates
     * data to the mirror. Each space instance has it's own replication channel to the mirror. For
     * example:
     *
     * <code> SpaceInstanceStatistics statistics = spaceInstance.getStatistics();
     *
     * MirrorStatistics mirrorStat =statistics.getMirrorStatistics();
     *
     * MirrorBulkStatistics primaryToMirrorStatistics = mirrorStat.getSourceChannelStatistics("mySpace_container1:mySpace");
     * long bulkCountFromPrimary = primaryToMirrorStatistics.getOperationCount(); long
     * writeCountFromPrimary = primaryToMirrorStatistics.getWriteOperationStatistics().getOperationCount();
     *
     * </code>
     *
     * @param channelName the name of the source mirror channel. Format : <space-containerName:spaceName>
     */
    public MirrorOperations getSourceChannelStatistics(String channelName);

    /**
     * Get mirror statistics for all space instances incoming channels to the mirror
     */
    public Map<String, ? extends MirrorOperations> getAllSourceChannelStatistics();

}
