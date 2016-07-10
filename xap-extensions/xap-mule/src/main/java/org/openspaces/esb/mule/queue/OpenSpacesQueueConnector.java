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


package org.openspaces.esb.mule.queue;

import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.endpoint.EndpointException;
import org.mule.api.endpoint.EndpointURI;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.api.transport.MessageReceiver;
import org.mule.endpoint.DynamicURIInboundEndpoint;
import org.mule.endpoint.MuleEndpointURI;
import org.mule.routing.filters.WildcardFilter;
import org.mule.transport.AbstractConnector;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Iterator;

/**
 * An OS queue connector. Holding the actual {@link org.openspaces.core.GigaSpace} instance that
 * will be used to communicate with the Space by the dispatcher, receiver, and requestor.
 *
 * <p>If the giga space reference is defined ({@link #setGigaSpace(String)}, will use it to find the
 * {@link org.openspaces.core.GigaSpace} instance defined. If it is not defined, will try to get
 * GigaSpace instances from Spring and if there is only one defined, will used it.
 *
 * <p>Also holds other attributes related to the written and read entry. Such as if the entry will
 * be a fifo one, and if it will be persisted.
 *
 * @author kimchy
 */
public class OpenSpacesQueueConnector extends AbstractConnector implements ApplicationContextAware {

    public static final String OS_QUEUE = "os-queue";


    private String gigaSpaceRef;

    private boolean fifo = false;

    private boolean persistent = false;

    private long timeout = 1000;

    private Integer batchSize;

    private ApplicationContext applicationContext;

    private GigaSpace gigaSpace;


    public OpenSpacesQueueConnector(MuleContext context) {
        super(context);
    }

    /**
     * @return the openspaces protocol name.
     */
    public String getProtocol() {
        return OS_QUEUE;
    }


    /**
     * Injected by Spring. The application context to get the {@link org.openspaces.core.GigaSpace}
     * instance from.
     */
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    /**
     * Sets the GigaSpace bean id reference to be retrieved from Spring. If not defined, will try to
     * get all the GigaSpace instances from Spring, and if there is only one, will use it.
     */
    public void setGigaSpace(String gigaSpaceRef) {
        this.gigaSpaceRef = gigaSpaceRef;
    }

    /**
     * Returns the GigaSpace bean id reference to be retrieved from Spring. If not defined, will try
     * to get all the GigaSpace instances from Spring, and if there is only one, will use it.
     */
    public String getGigaSpace() {
        return gigaSpaceRef;
    }

    /**
     * Should the entries written to the virtualized queue be fifo or not. Defaults to
     * <code>false</code>.
     */
    public boolean isFifo() {
        return fifo;
    }

    /**
     * Should the entries written to the virtualized queue be fifo or not. Defaults to
     * <code>false</code>.
     */
    public void setFifo(boolean fifo) {
        this.fifo = fifo;
    }

    /**
     * Should the entries written to the Space will be persistent to a backend storage or not.
     * Defaults to <code>false</code> (as many times a backup space is enough).
     */
    public boolean isPersistent() {
        return persistent;
    }

    /**
     * Should the entries written to the Space will be persistent to a backend storage or not.
     * Defaults to <code>false</code> (as many times a backup space is enough).
     */
    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    /**
     * The timeout waiting for a message on the queue in <b>milliseconds</b>. Defaults to 1000.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * The timeout waiting for a message on the queue in <b>milliseconds</b>. Defaults to 1000.
     */
    public void setTimeout(long timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout cannot be negative");
        }
        this.timeout = timeout;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        if (batchSize < 0) {
            throw new IllegalArgumentException("batchSize cannot be negative");
        }
        this.batchSize = batchSize;
    }

    @Override
    protected void doInitialise() throws InitialisationException {
        if (batchSize == null) {
            // This is how batch size was determined prior to 9.7.0
            int maxThreads = getReceiverThreadingProfile().getMaxThreadsActive();
            batchSize = Math.max(1, ((maxThreads / 2) - 1));
        }
    }

    @Override
    protected void doDispose() {
    }

    @Override
    protected void doStart() throws MuleException {
    }

    @Override
    protected void doStop() throws MuleException {
    }

    @Override
    protected void doConnect() throws Exception {
        if (gigaSpaceRef == null) {
            String[] beansNames = applicationContext.getBeanNamesForType(GigaSpace.class);
            if (beansNames != null && beansNames.length == 1) {
                gigaSpace = (GigaSpace) applicationContext.getBean(beansNames[0]);
            } else {
                throw new RuntimeException("No GigaSpace ref is configured, and more than one GigaSpace bean is configured");
            }
        } else {
            gigaSpace = (GigaSpace) applicationContext.getBean(gigaSpaceRef);
        }
    }

    @Override
    protected void doDisconnect() throws Exception {
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public GigaSpace getGigaSpaceObj() {
        return gigaSpace;
    }

    OpenSpacesQueueMessageReceiver getReceiver(EndpointURI endpointUri) throws EndpointException {
        return (OpenSpacesQueueMessageReceiver) getReceiverByEndpoint(endpointUri);
    }

    protected MessageReceiver getReceiverByEndpoint(EndpointURI endpointUri) throws EndpointException {
        if (logger.isDebugEnabled()) {
            logger.debug("Looking up os-queue receiver for address: " + endpointUri.toString());
        }

        MessageReceiver receiver;
        // If we have an exact match, use it
        receiver = receivers.get(endpointUri.getAddress());
        if (receiver != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Found exact receiver match on endpointUri: " + endpointUri);
            }
            return receiver;
        }

        // otherwise check each one against a wildcard match
        for (Iterator iterator = receivers.values().iterator(); iterator.hasNext(); ) {
            receiver = (MessageReceiver) iterator.next();
            String filterAddress = receiver.getEndpointURI().getAddress();
            WildcardFilter filter = new WildcardFilter(filterAddress);
            if (filter.accept(endpointUri.getAddress())) {
                InboundEndpoint endpoint = receiver.getEndpoint();
                EndpointURI newEndpointURI = new MuleEndpointURI(endpointUri, filterAddress);
                receiver.setEndpoint(new DynamicURIInboundEndpoint(endpoint, newEndpointURI));

                if (logger.isDebugEnabled()) {
                    logger.debug("Found receiver match on endpointUri: " + receiver.getEndpointURI()
                            + " against " + endpointUri);
                }
                return receiver;
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("No receiver found for endpointUri: " + endpointUri);
        }
        return null;
    }

    /**
     * Creates a new template for querying the queue
     *
     * @return OpenSpacesQueueObject
     */
    public OpenSpacesQueueObject newQueueTemplate(String queueName) {
        OpenSpacesQueueObject queueObject = isFifo() ? new OpenSpacesFifoQueueObject() : new OpenSpacesQueueObject();
        queueObject.setPersistent(isPersistent());
        queueObject.setEndpointURI(queueName);
        return queueObject;
    }

    /**
     * Creates a new entry to put in the queue
     *
     * @return OpenSpacesQueueObject
     */
    public OpenSpacesQueueObject newQueueEntry(String queueName) {
        OpenSpacesQueueObject queueObject = isFifo() ? new OpenSpacesFifoQueueObject() : new OpenSpacesQueueObject();
        queueObject.setPersistent(isPersistent());
        queueObject.setEndpointURI(queueName);
        return queueObject;
    }

    @Override
    public boolean isResponseEnabled() {
        return true;
    }
}
