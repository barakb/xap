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


/**
 * Monitoring details of a specific method
 *
 * @author eitany
 * @see LRMIServiceClientMonitoringDetails
 * @since 9.1
 */
public interface LRMIMethodMonitoringDetails {

    /**
     * Gets the number of time this method was invoked
     */
    public long getInvocationCount();

    /**
     * Gets the total network traffic that was sent as a response to this method invocation
     */
    public long getGeneratedTraffic();

    /**
     * Gets the total network traffic that was received for this method invocation
     */
    public long getReceivedTraffic();

}