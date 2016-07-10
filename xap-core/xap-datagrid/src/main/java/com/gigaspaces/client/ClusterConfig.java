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

package com.gigaspaces.client;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class ClusterConfig {

    private String schema;
    private Integer instanceId;
    private Integer backupId;
    private Integer numberOfInstances;
    private Integer numberOfBackups;

    public String getSchema() {
        return schema;
    }

    public ClusterConfig setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public Integer getInstanceId() {
        return instanceId;
    }

    public ClusterConfig setInstanceId(Integer instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public Integer getBackupId() {
        return backupId;
    }

    public ClusterConfig setBackupId(Integer backupId) {
        this.backupId = backupId;
        return this;
    }

    public Integer getNumberOfInstances() {
        return numberOfInstances;
    }

    public ClusterConfig setNumberOfInstances(Integer numberOfInstances) {
        this.numberOfInstances = numberOfInstances;
        return this;
    }

    public Integer getNumberOfBackups() {
        return numberOfBackups;
    }

    public ClusterConfig setNumberOfBackups(Integer numberOfBackups) {
        this.numberOfBackups = numberOfBackups;
        return this;
    }
}
