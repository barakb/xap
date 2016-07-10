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

import com.gigaspaces.document.DocumentProperties;

import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.EndpointURI;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.api.execution.ExecutionCallback;
import org.mule.api.execution.ExecutionTemplate;
import org.mule.api.processor.MessageProcessor;
import org.mule.api.transaction.TransactionConfig;
import org.mule.api.transport.DispatchException;
import org.mule.api.transport.PropertyScope;
import org.mule.config.i18n.CoreMessages;
import org.mule.config.i18n.Message;
import org.mule.execution.TransactionalErrorHandlingExecutionTemplate;
import org.mule.transport.AbstractMessageDispatcher;

import java.io.IOException;
import java.util.UUID;


/**
 * Dispatches (writes) a message to an internal queue. The queue is a virtualized queue represented
 * by the {@link org.openspaces.esb.mule.queue.OpenSpacesQueueObject} with its endpoint address set
 * (and not the message).
 *
 * @author kimchy
 */
public class OpenSpacesQueueMessageDispatcher extends AbstractMessageDispatcher {

    public static final String DEFAULT_RESPONSE_QUEUE = "_response_queue";

    private final OpenSpacesQueueConnector connector;

    public OpenSpacesQueueMessageDispatcher(OutboundEndpoint endpoint) {
        super(endpoint);
        this.connector = (OpenSpacesQueueConnector) endpoint.getConnector();
    }

    protected void doDispatch(final MuleEvent event) throws Exception {
        dispatchMessage(event, false);
    }

    protected MuleMessage doSend(final MuleEvent event) throws Exception {
        return dispatchMessage(event, true);
    }

    private MuleMessage dispatchMessage(final MuleEvent event, boolean doSend) throws Exception {
        final EndpointURI endpointUri = endpoint.getEndpointURI();

        if (endpointUri == null) {
            Message objectIsNull = CoreMessages.objectIsNull("Endpoint");
            DispatchException ex = null;
            if (endpoint instanceof MessageProcessor) {
                ex = new DispatchException(objectIsNull, event, (MessageProcessor) endpoint, new Exception());
            } else {
                ex = new DispatchException(objectIsNull, event, null, new Exception());
            }
            throw ex;
        }
        final OpenSpacesQueueMessageReceiver receiver = connector.getReceiver(endpointUri);

        final TransactionConfig transactionConfig = receiver == null ? endpoint.getTransactionConfig() : receiver.getEndpoint()
                .getTransactionConfig();
        final ExecutionTemplate<MuleEvent> executionTemplate = TransactionalErrorHandlingExecutionTemplate
                .createMainExecutionTemplate(event.getMuleContext(), transactionConfig);

        connector.getSessionHandler().storeSessionInfoToMessage(event.getSession(), event.getMessage());

        //handle transactional operations - don't put on queue - just execute recursively
        // note - transactions works only in the same mule scope.
        boolean isTransactional = endpoint.getTransactionConfig().isTransacted();
        if (isTransactional && receiver != null) {
            ExecutionCallback<MuleEvent> executionCallback = new ExecutionCallback<MuleEvent>() {
                @Override
                public MuleEvent process() throws Exception {
                    return (MuleEvent) receiver.onCall(event.getMessage(), true);
                }
            };
            MuleEvent muleEvent = executionTemplate.execute(executionCallback);
            return muleEvent != null ? muleEvent.getMessage() : null;
        }

        //check if a response should be returned for this endpoint
        final boolean returnResponse = returnResponse(event, doSend) && !isTransactional;

        //assign correlationId for sync invocations - so that the request can be correlated with the response
        final String correlationId = createCorrelationIdIfNotExists(event);

        MuleMessage message = event.getMessage();
        connector.getSessionHandler().storeSessionInfoToMessage(event.getSession(), message);

        final ExecutionCallback<MuleEvent> executionCallback = new ExecutionCallback<MuleEvent>() {
            @Override
            public MuleEvent process() throws Exception {
                OpenSpacesQueueObject entry = prepareMessageForDispatch(
                        event.getMessage(), endpointUri, correlationId, returnResponse);
                connector.getGigaSpaceObj().write(entry);
                return null;
            }
        };

        executionTemplate.execute(executionCallback);

        if (logger.isDebugEnabled()) {
            logger.debug("sent event on endpointUri: " + endpoint.getEndpointURI());
        }

        // wait for reply if configured
        if (returnResponse) {
            return waitForResponse(event, correlationId);
        }
        return null;
    }

    private String createCorrelationIdIfNotExists(final MuleEvent event) {
        String correlationId = event.getMessage().getCorrelationId();
        if (correlationId == null || correlationId.trim().length() == 0)
            correlationId = UUID.randomUUID().toString();
        return correlationId;
    }

    private OpenSpacesQueueObject prepareMessageForDispatch(final MuleMessage message,
                                                            final EndpointURI endpointUri,
                                                            final String correlationId,
                                                            final boolean returnResponse) throws IOException {
        OpenSpacesQueueObject entry = connector.newQueueEntry(endpointUri.getAddress());
        entry.setCorrelationID(correlationId);
        entry.setPayload(message.getPayload());

        //copy the message properties
        DocumentProperties payloadMetaData = new DocumentProperties();
        for (String propertyName : message.getPropertyNames(PropertyScope.OUTBOUND)) {
            payloadMetaData.put(propertyName, message.getProperty(propertyName, PropertyScope.OUTBOUND));
        }
        if (returnResponse) {
            payloadMetaData.put(OpenSpacesQueueObject.RESPONSE_TIMEOUT_PROPERTY, endpoint.getResponseTimeout());
        }
        entry.setPayloadMetaData(payloadMetaData);
        return entry;
    }

    private MuleMessage waitForResponse(final MuleEvent event, final String correlationId) throws Exception {
        String replyTo = endpoint.getEndpointURI().getAddress() + DEFAULT_RESPONSE_QUEUE;

        int timeout = endpoint.getResponseTimeout();

        if (logger.isDebugEnabled()) {
            logger.debug("waiting for response Event on endpointUri: " + replyTo);
        }

        OpenSpacesQueueObject template = connector.newQueueTemplate(replyTo);
        template.setCorrelationID(correlationId);

        try {
            OpenSpacesQueueObject responseEntry = connector.getGigaSpaceObj().take(template, timeout);
            if (logger.isDebugEnabled()) {
                logger.debug("got response Event on endpointUri: " + replyTo + " response=" + responseEntry);
            }
            if (responseEntry == null)
                throw new DispatchException(event, getEndpoint());
            return createMuleMessage(responseEntry);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("got no response Event on endpointUri: " + replyTo);
            }
            throw e;
        }
    }

    protected void doDispose() {
        // template method
    }

    protected void doConnect() throws Exception {
        // template method
    }

    protected void doDisconnect() throws Exception {
        // template method
    }

}
