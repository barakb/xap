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

import net.jini.config.Configuration;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.lookup.entry.Name;

import java.util.Vector;

/**
 * The AbstractFaultDetectionHandler provides a base class which can be extended to provide concrete
 * FaultDetectionHandler capabilities. The basic infrastructure included in the
 * AbstractFaultDetectionHandler includes {@link org.jini.rio.core.FaultDetectionListener}
 * registration and notification, a {@link net.jini.lookup.ServiceDiscoveryListener} implementation
 *
 * Properties that will be common across all classes which extend this class are also provided: <ul>
 * <li>retryCount <li>retryTimeout </ul>
 *
 * @see org.jini.rio.core.FaultDetectionHandler
 * @see org.jini.rio.core.FaultDetectionListener
 */
public abstract class AbstractFaultDetectionHandler implements FaultDetectionHandler {
    public static final int DEFAULT_RETRY_COUNT = 3;
    public static final long DEFAULT_RETRY_TIMEOUT = 1000 * 1;
    public static final String RETRY_COUNT_KEY = "retryCount";
    public static final String RETRY_TIMEOUT_KEY = "retryTimeout";
    /**
     * Object that can be used to communicate to the service
     */
    protected Object proxy;
    protected int retryCount = DEFAULT_RETRY_COUNT;
    protected long retryTimeout = DEFAULT_RETRY_TIMEOUT;
    /**
     * Collection of FaultDetectionListeners
     */
    private Vector<FaultDetectionListener> listeners = new Vector<FaultDetectionListener>();
    /**
     * ServiceID used to discover service instance
     */
    private ServiceID serviceID;
    /**
     * Flag to indicate the utility is terminating
     */
    protected boolean terminating = false;
    /**
     * Configuration arguments
     */
    protected String[] configArgs;
    /**
     * A Configuration object
     */
    protected Configuration config;

    /**
     * Class which provides service monitoring
     */
    protected ServiceMonitor serviceMonitor;

    /**
     * @see org.jini.rio.core.FaultDetectionHandler#register
     */
    @Override
    public void register(FaultDetectionListener listener) {
        if (listener == null)
            throw new NullPointerException("listener is null");
        if (!listeners.contains(listener))
            listeners.addElement(listener);
    }

    /**
     * @see org.jini.rio.core.FaultDetectionHandler#monitor
     */
    @Override
    public void monitor(Object proxy, ServiceID serviceID)
            throws Exception {
        if (proxy == null)
            throw new NullPointerException("proxy is null");
        if (serviceID == null)
            throw new NullPointerException("serviceID is null");
        this.proxy = proxy;
        this.serviceID = serviceID;

        monitor();
    }

    /**
     * Get the class which implements the ServiceMonitor
     */
    protected abstract void monitor() throws Exception;

    /**
     * @see org.jini.rio.core.FaultDetectionHandler#terminate
     */
    @Override
    synchronized public void terminate() {
        if (terminating)
            return;
        terminating = true;

        if (serviceMonitor != null) {
            serviceMonitor.drop();
        }
    }

    /**
     * Notify FaultDetectionListener instances the service has been removed
     */
    protected void notifyListeners() {

        Object[] arrLocal = listeners.toArray();
        for (int i = arrLocal.length - 1; i >= 0; i--)
            ((FaultDetectionListener) arrLocal[i]).serviceFailure(proxy, serviceID);
    }

    /**
     * Defines the semantics of an internal class which will be used in perform service-specific
     * monitoring
     */
    public interface ServiceMonitor {
        /**
         * Drop the Monitor, its not longer needed
         */
        void drop();

        /**
         * Verify that the service can be reached. If the service cannot be reached return false
         */
        boolean verify();
    }


    /**
     * Get the first Name.name from the attribute collection set
     */
    protected String getName(Entry[] attrs) {
        for (int i = 0; i < attrs.length; i++) {
            if (attrs[i] instanceof Name) {
                return (((Name) attrs[i]).name);
            }
        }
        return ("unknown");
    }
}