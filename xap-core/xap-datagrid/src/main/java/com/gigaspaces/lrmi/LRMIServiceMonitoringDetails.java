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

package com.gigaspaces.lrmi;

import java.util.Map;

/**
 * Monitoring details of a single hosted service
 *
 * @author eitany
 * @since 9.1
 */
public interface LRMIServiceMonitoringDetails {

    /**
     * Gets the service details
     */
    public String getServiceDetails();

    /**
     * Gets the class loader which holds the service.
     */
    public String getServiceClassLoaderDetails();

    /**
     * Gets the service remote id.
     */
    public long getRemoteObjID();

    /**
     * Gets the service connection url.
     */
    public String getConnectionUrl();

    /**
     * Gets a mapping of tracking details of each client connected to this service.
     */
    public Map<LRMIServiceClientMonitoringId, LRMIServiceClientMonitoringDetails> getClientsTrackingDetails();

}