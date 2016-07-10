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


package com.gigaspaces.datasource;

import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

import java.util.Properties;

/**
 * ManagedDataSource should be implemented when initialization and shutdown operations are needed by
 * the data source.<br><br>
 *
 * Usually in case of external databases - <tt>init()</tt> will establish a JDBC connection and
 * <tt>shutdown()</tt> will close it.<br><br>
 *
 * If the implementation requires configuration, it can be configured in external properties file.
 * <br> The content of this file is passed to <tt> init() </tt> The location of the properties file
 * should be defined in the space schema or property:<br><br>
 *
 * <b>space-config.ExternalDataSource.init-properties-file</b>
 *
 * @author anna
 * @see DataProvider
 * @see DataPersister
 * @since 6.0
 * @deprecated since 9.5 - use {@link SpaceDataSource} or {@link SpaceSynchronizationEndpoint}
 * instead.
 */
@Deprecated
public interface ManagedDataSource<T> {
    /**
     * use this constant to get from the Properties the number of partitions
     */
    String NUMBER_OF_PARTITIONS = "com.gigaspaces.datasource.number-of-partitions";
    /**
     * use this constant to get from the Properties the partition number
     */
    String STATIC_PARTITION_NUMBER = "com.gigaspaces.datasource.partition-number";

    /**
     * Initialize and configure the data source using given properties.<br> Called when space is
     * started.<br>
     *
     * The properties are loaded from a file that can be defined in the space schema or as a
     * property named:<br><br>
     *
     * space-config.ExternalDataSource.properties-file
     *
     * partitionId and number of partitions are also in the Properties - can be read with
     * STATIC_PARTITION_NUMBER and NUMBER_OF_PARTITIONS
     *
     * @param prop - contains user defined param and Partition data
     */
    void init(Properties prop) throws DataSourceException;


    /**
     * <code> Creates and returns an iterator over all the entries that should be loaded into
     * space.
     *
     * </code>
     *
     * @return a {@link DataIterator} or null if no data should be loaded into space
     */
    public DataIterator<T> initialLoad() throws DataSourceException;

    /**
     * Close the data source and clean any used resources.<br> Called before space shutdown.<br>
     */
    void shutdown() throws DataSourceException;

}
