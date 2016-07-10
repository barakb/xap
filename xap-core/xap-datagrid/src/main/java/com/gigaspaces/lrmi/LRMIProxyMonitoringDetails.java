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

import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.util.Map;

/**
 * Monitoring details of a single remote proxy.
 *
 * @author eitany
 * @since 9.1
 */
public interface LRMIProxyMonitoringDetails {
    /**
     * Gets the total network traffic that was received by this proxy.
     */
    public long getTotalReceivedTraffic();

    /**
     * Gets the total network traffic that was sent by this proxy.
     */
    public long getTotalGeneratedTraffic();

    /**
     * Gets the connection url of this proxy endpoint.
     */
    public String getConnectionUrl();

    /**
     * Gets the details of the service behind this proxy implements.
     */
    public String getServiceDetails();

    /**
     * Gets the service version
     */
    public PlatformLogicalVersion getServiceVersion();

    /**
     * Gets the pid inside the host of the process hosting this service
     *
     * @since 9.7
     */
    public long getServicePid();

    /**
     * Gets the hostname of the machine hosting this service
     *
     * @since 9.7
     */
    public String getServiceHostname();

    /**
     * Gets tracking details per method type.
     */
    public Map<String, LRMIMethodMonitoringDetails> getTrackingDetails();
}
