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
package com.gigaspaces.cluster;

import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.failuredetector.IFailureDetector;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.j_spaces.core.cluster.startup.FaultDetectionListener;
import com.j_spaces.core.cluster.startup.GigaSpacesFaultDetectionHandler;

import net.jini.core.lookup.ServiceID;

import java.util.HashMap;
import java.util.Map;


/**
 * Monitors cluster members proxies and detects proxy disconnections. Once proxy was disconnected it
 * is removed from monitoring. When disconnected member reconnects it will be re-registered as a new
 * service.
 *
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ClusterFailureDetector
        implements FaultDetectionListener, IFailureDetector {
    private final Map<ServiceID, GigaSpacesFaultDetectionHandler> _failureDetectorsByID = new HashMap<ServiceID, GigaSpacesFaultDetectionHandler>();

    private final String[] _fdhConfig;

    public static final int DEFAULT_RETRY_COUNT = 3;
    public static final long DEFAULT_RETRY_TIMEOUT = 100;
    public static final long DEFAULT_INVOCATION_DELAY = 1000;

    /**
     *
     */
    public ClusterFailureDetector() {
        String className = GigaSpacesFaultDetectionHandler.class.getName();
        _fdhConfig = new String[4];
        _fdhConfig[0] = "-";
        _fdhConfig[1] = className + ".retryCount=" + DEFAULT_RETRY_COUNT;
        _fdhConfig[2] = className + ".invocationDelay=" + DEFAULT_INVOCATION_DELAY;
        _fdhConfig[3] = className + ".retryTimeout=" + DEFAULT_RETRY_TIMEOUT;

    }

    public ClusterFailureDetector(String[] config) {
        _fdhConfig = config;

    }

    @Override
    public void registerRemoteSpace(FaultDetectionListener listener, ServiceID serviceId,
                                    IRemoteSpace spaceProxy) throws Exception {
        register(listener, serviceId, spaceProxy);
    }

    public synchronized void register(FaultDetectionListener listener,
                                      ServiceID serviceId, Object service) throws Exception {
        GigaSpacesFaultDetectionHandler detector = _failureDetectorsByID.get(serviceId);

        if (detector == null) {
            detector = new GigaSpacesFaultDetectionHandler();
            detector.setConfiguration(_fdhConfig);

            _failureDetectorsByID.put(serviceId, detector);

            // register for failure events to know when to clear the table
            detector.register(this);


        }


        detector.register(listener);
        /* 
         * Add the stub to fault detector to receive failure events.
         */
        try {
            detector.monitor(service, serviceId);
        } catch (Exception e) {
            serviceFailure(service, serviceId);
            throw e;
        }
    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.cluster.startup.FaultDetectionListener#serviceFailure(java.lang.Object, java.lang.Object)
     */
    @Override
    public synchronized void serviceFailure(Object service, Object serviceID) {
        GigaSpacesFaultDetectionHandler fHandler = _failureDetectorsByID.get(serviceID);
        if (fHandler != null && fHandler.getSpaceProxy() == service)
            _failureDetectorsByID.remove(serviceID);

    }

    public synchronized void terminate() {
        for (GigaSpacesFaultDetectionHandler detector : _failureDetectorsByID.values()) {
            detector.terminate();
        }

        _failureDetectorsByID.clear();
    }

}
