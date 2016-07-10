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

package com.gigaspaces.server.blobstore;

import com.gigaspaces.metrics.MetricRegistrator;

import java.util.Properties;

/**
 * This class is used to inject configuration into a blobstore implementation. <br> it contains a
 * set of configuration parameters which are needed to a blobstore implementation:
 * <tt>spaceName</tt>  - current space name  <br> <tt>properties</tt> - properties that injected at
 * the space.<br> <tt>warmStart</tt> - Denotes if we need to load data from storage. <br>
 * <tt>metricRegistrator</tt>  - using metrics to expose relevant blobstore implementation
 * statistics. <br>
 *
 * @author kobi on 8/6/15.
 * @see com.gigaspaces.server.blobstore.BlobStoreStorageHandler
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreConfig {

    private final String spaceName;
    private final int numberOfPartitions;
    private final int numberOfBackups;
    private boolean warmStart;
    private final MetricRegistrator metricRegistrator;
    private final Properties properties;

    public BlobStoreConfig(String spaceName, int numberOfPartitions, int numberOfBackups, boolean warmStart,
                           MetricRegistrator metricRegistrator) {
        this.spaceName = spaceName;
        this.numberOfPartitions = numberOfPartitions;
        this.numberOfBackups = numberOfBackups;
        this.warmStart = warmStart;
        this.metricRegistrator = metricRegistrator;
        this.properties = new Properties();
        this.properties.put("numberOfPartitions", numberOfPartitions);
        this.properties.put("numberOfBackups", numberOfBackups);
    }

    public String getSpaceName() {
        return spaceName;
    }

    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }

    public int getNumberOfBackups() {
        return numberOfBackups;
    }

    public boolean isWarmStart() {
        return warmStart;
    }

    public void setWarmStart(boolean warmStart) {
        this.warmStart = warmStart;
    }

    public MetricRegistrator getMetricRegistrator() {
        return metricRegistrator;
    }

    public Properties getProperties() {
        return properties;
    }
}
