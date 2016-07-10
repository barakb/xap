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

package com.j_spaces.core.cluster.startup;

import net.jini.core.lookup.ServiceID;

/**
 * The FaultDetectionHandler is loaded by client entities do determine whether a service is
 * reachable. Developers may choose to implement custom fault detection algorithms and protocols to
 * determine service reachability and use those approaches in concrete implementations of this
 * interface
 */
public interface FaultDetectionHandler {
    /**
     * Set configuration attributes for the FaultDetectionHandler.
     *
     * @param configArgs Configuration attributes a FaultDetectionHandler will use to monitor the
     *                   service. Values are specific to a concrete instance of the
     *                   FaultDetectionHandler
     */
    void setConfiguration(String[] configArgs);

    /**
     * Register a FaultDetectionListener
     *
     * @param listener The FaultDetectionListener to register
     */
    void register(FaultDetectionListener listener);

    /**
     * Begin monitoring the service
     *
     * @param service   The service that the FaultDetectionHandler will monitor, must not be
     *                  <code>null</code>
     * @param serviceID An Object representing a unique service identifier for the service being
     *                  monitored, must not be <code>null</code>
     * @throws Exception            If there are abnormal conditions encountered
     * @throws NullPointerException if required parameters are <code>null</code>
     */
    void monitor(Object service, ServiceID serviceID)
            throws Exception;

    /**
     * Terminate the FaultDetectionHandler
     */
    void terminate();
}