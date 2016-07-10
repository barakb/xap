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

package com.gigaspaces.lrmi.nio.info;

import com.gigaspaces.annotation.lrmi.MonitoringPriority;
import com.gigaspaces.lrmi.LRMIMonitoringDetails;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * A general interface for services that can provide remote NIO information.
 *
 * @author kimchy
 */
public interface NIOInfoProvider extends Remote {

    /**
     * Returns the transport configuration. Note, does not change during application lifecycle,
     * should be cached on the client side.
     */
    @MonitoringPriority
    NIODetails getNIODetails() throws RemoteException;

    /**
     * Returns the transport statistics.
     */
    @MonitoringPriority
    NIOStatistics getNIOStatistics() throws RemoteException;

    /**
     * Enables lrmi monitoring (gigaspaces internal remoting layer), this will cause the target to
     * start track lrmi invocations which can later be viewed by calling. {@link
     * #fetchLRMIMonitoringDetails()}
     */
    @MonitoringPriority
    void enableLRMIMonitoring() throws RemoteException;

    /**
     * Disabled lrmi monitoring (gigaspaces internal remoting layer). {@link
     * #enableLRMIMonitoring()}
     */
    @MonitoringPriority
    void disableLRMIMonitoring() throws RemoteException;

    /**
     * Return lrmi (gigaspaces internal remoting layer) services monitoring details. This will only
     * work if lrmi monitoring was previously enabled, either by admin API, Jmx or system property.
     * {@link #enableLRMIMonitoring()}
     */
    @MonitoringPriority
    LRMIMonitoringDetails fetchLRMIMonitoringDetails() throws RemoteException;
}
