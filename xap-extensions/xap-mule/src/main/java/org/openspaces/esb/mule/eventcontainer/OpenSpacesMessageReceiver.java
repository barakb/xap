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


package org.openspaces.esb.mule.eventcontainer;

import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.MuleRuntimeException;
import org.mule.api.construct.FlowConstruct;
import org.mule.api.endpoint.ImmutableEndpoint;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.exception.MessagingExceptionHandler;
import org.mule.api.execution.ExecutionCallback;
import org.mule.api.execution.ExecutionTemplate;
import org.mule.api.lifecycle.CreateException;
import org.mule.api.service.Service;
import org.mule.api.transaction.Transaction;
import org.mule.api.transaction.TransactionException;
import org.mule.api.transport.Connector;
import org.mule.config.i18n.CoreMessages;
import org.mule.exception.DefaultMessagingExceptionStrategy;
import org.mule.transport.AbstractMessageReceiver;
import org.mule.transport.AbstractReceiverWorker;
import org.openspaces.core.GigaSpace;
import org.openspaces.events.AbstractEventListenerContainer;
import org.openspaces.events.SpaceDataEventListener;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.TransactionStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <code>OpenSpacesMessageReceiver</code> is used to receive data from an GigaSpaces's space. It
 * implements SpaceDataEventListener and as such it register itself to Polling/Notify
 * SpaceListeningContainer.
 *
 * @author yitzhaki
 * @see org.openspaces.events.SpaceDataEventListener
 */
public class OpenSpacesMessageReceiver extends AbstractMessageReceiver implements SpaceDataEventListener {

    private static final String ENDPOINT_PARAM_WORK_MANAGER = "workManager";
    private static final MessagingExceptionHandler exceptionHandler = new DefaultMessagingExceptionStrategy();

    private AbstractEventListenerContainer eventListenerContainer;

    private boolean workManager = false;

    private volatile boolean disposed = false;

    /**
     * Creates a OpenSpacesMessageReceiver and resister it as a SpaceDataEventListener to the
     * Polling/Notify container that declared as umoEndpoint.EndpointURI.address.
     *
     * @param connector the endpoint that created this listener
     * @param endpoint  the provider contains the endpointUri on which the receiver will listen on.
     *                  The URI structure must be declared as the following os://<Polling/Notify
     *                  container id>
     * @see ImmutableEndpoint
     */

    public OpenSpacesMessageReceiver(Connector connector, FlowConstruct flowConstruct, InboundEndpoint endpoint) throws CreateException {
        super(connector, flowConstruct, endpoint);
        init(connector, endpoint);
    }

    public OpenSpacesMessageReceiver(Connector connector, Service service, InboundEndpoint endpoint) throws CreateException {
        super(connector, service, endpoint);
        init(connector, endpoint);
    }

    private void init(Connector connector, InboundEndpoint endpoint) throws CreateException {
        ApplicationContext applicationContext = ((OpenSpacesConnector) connector).getApplicationContext();
        if (applicationContext == null) {
            throw new CreateException(CoreMessages.connectorWithProtocolNotRegistered(connector.getProtocol()), this);
        }

        initWritingAttributes(endpoint);
        String eventListenerContainerName = endpoint.getEndpointURI().getAddress();
        eventListenerContainer = (AbstractEventListenerContainer) applicationContext.getBean(eventListenerContainerName);
        eventListenerContainer.setEventListener(this);
    }

    /**
     * Extract the workManager setting from the URI. If the atrribute is missing sets it to the
     * default (<code>false</code>).
     */
    private void initWritingAttributes(ImmutableEndpoint endpoint) {
        Properties params = endpoint.getEndpointURI().getParams();
        if (params != null) {
            try {
                String workManager = (String) params.get(ENDPOINT_PARAM_WORK_MANAGER);
                if (workManager != null) {
                    this.workManager = Boolean.valueOf(workManager);
                }
            } catch (Exception e) {
                throw new MuleRuntimeException(CoreMessages.failedToCreateConnectorFromUri(endpoint.getEndpointURI()), e);
            }
        }
    }

    /**
     * An event callback with the actual data object of the event. This method invoked by
     * eventListenerContainer creates a UMOMessage that wraps the data object, then routes the
     * UMOMessage via the Mule bus.
     *
     * @param data      The actual data object of the event
     * @param gigaSpace A GigaSpace instance that can be used to perofrm additional operations
     *                  against the space
     * @param txStatus  An optional transaction status allowing to rollback a transaction
     *                  programmatically
     * @param source    Optional additional data or the actual source event data object (where
     *                  relevant)
     */
    public void onEvent(final Object data, final GigaSpace gigaSpace, final TransactionStatus txStatus, final Object source) {
        if (txStatus != null) {
            final ExecutionTemplate<MuleEvent> executionTemplate = createExecutionTemplate();
            try {
                if (disposed) {
                    txStatus.setRollbackOnly();
                    return;
                }
                executionTemplate.execute(new ExecutionCallback<MuleEvent>() {
                    @Override
                    public MuleEvent process() throws Exception {
                        return doReceiveEvent(data, gigaSpace, txStatus, source);
                    }
                });
            } catch (Exception e) {
                txStatus.setRollbackOnly();
            }
        } else {
            if (disposed) {
                return;
            }
            try {
                doReceiveEvent(data, gigaSpace, txStatus, source);
            } catch (Exception e) {
                getConnector().getMuleContext().handleException(e);
            }
        }
    }


    protected MuleEvent doReceiveEvent(Object data, GigaSpace gigaSpace, TransactionStatus txStatus,
                                       Object source) throws Exception {
        if (workManager) {
            getWorkManager().scheduleWork(new GigaSpaceWorker(data, this, gigaSpace));
        } else {
            final MuleMessage message = createMuleMessage(data, endpoint.getEncoding());
            final MuleEvent muleEvent = routeMessage(message);
            //write response 
            //should send back only if remote synch is set or no outbound endpoints
            if (endpoint.getExchangePattern().hasResponse()) {
                MuleMessage responseMessage = muleEvent.getMessage();
                Object payload = responseMessage.getPayload();

                if (logger.isDebugEnabled()) {
                    logger.debug(getEndpointURI() + " sending response to client  " + payload);
                }
                writeResponseToSpace(gigaSpace, payload);
            }
            return muleEvent;
        }
        return null;
    }

    private static void writeResponseToSpace(GigaSpace gigaSpace, Object payload) {

        if (payload != null) {
            if (payload instanceof Object[]) {
                Object[] payloadArr = (Object[]) payload;
                gigaSpace.writeMultiple(payloadArr);
            } else {
                gigaSpace.write(payload);
            }
        }
    }

    protected static class GigaSpaceWorker extends AbstractReceiverWorker {

        private final GigaSpace gigaSpace;

        public GigaSpaceWorker(Object message, OpenSpacesMessageReceiver receiver, GigaSpace gigaSpace) {
            super(new ArrayList<Object>(1), receiver);
            this.gigaSpace = gigaSpace;
            messages.add(message);
        }

        protected void bindTransaction(Transaction tx) throws TransactionException {
            //TODO support transaction
        }

        @Override
        protected void handleResults(List messages) throws Exception {
            // write response
            // should send back only if remote synch is set or no outbound endpoints
            if (endpoint.getExchangePattern().hasResponse()) {
                for (Object message : messages) {
                    MuleMessage responseMessage = (MuleMessage) message;
                    Object payload = responseMessage.getPayload();
                    writeResponseToSpace(gigaSpace, payload);
                }
            }
        }
    }

    protected void doStart() throws MuleException {
        eventListenerContainer.start();
    }

    protected void doDisconnect() throws Exception {
        eventListenerContainer.setEventListener(null);
    }

    protected void doStop() throws MuleException {
        eventListenerContainer.stop();
    }

    protected void doConnect() throws Exception {
    }

    protected void doDispose() {
        disposed = true;
    }
}
