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

import com.j_spaces.core.client.UpdateModifiers;

import net.jini.core.lease.Lease;
import net.jini.space.JavaSpace;

import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.MuleRuntimeException;
import org.mule.api.endpoint.ImmutableEndpoint;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.api.lifecycle.CreateException;
import org.mule.config.i18n.CoreMessages;
import org.mule.transport.AbstractMessageDispatcher;
import org.openspaces.core.GigaSpace;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * <code>OpenSpacesMessageDispatcher</code> is responsible for sending messages to GigaSpaces
 * space.
 *
 * @author yitzhaki
 */
public class OpenSpacesMessageDispatcher extends AbstractMessageDispatcher {

    private static final String ENDPOINT_PARAM_WRITE_LEASE = "writeLease";

    private static final String ENDPOINT_PARAM_UPDATE_OR_WRITE = "updateOrWrite";

    private static final String ENDPOINT_PARAM_UPDATE_TIMEOUT = "updateTimeout";

    private GigaSpace gigaSpace;

    private long writeLease = Lease.FOREVER;

    private boolean updateOrWrite = true;

    private long updateTimeout = JavaSpace.NO_WAIT;


    public OpenSpacesMessageDispatcher(OutboundEndpoint endpoint) throws CreateException {
        super(endpoint);
        ApplicationContext applicationContext = ((OpenSpacesConnector) getConnector()).getApplicationContext();
        if (applicationContext == null) {
            throw new CreateException(CoreMessages.connectorWithProtocolNotRegistered(connector.getProtocol()), this);
        }
        initWritingAttributes(endpoint);
        String spaceId = endpoint.getEndpointURI().getPath();
        if (!StringUtils.hasLength(spaceId)) {
            spaceId = endpoint.getEndpointURI().getAddress();
        } else {
            if (spaceId.startsWith("/")) {
                spaceId = spaceId.substring(1);
            }
        }
        gigaSpace = (GigaSpace) applicationContext.getBean(spaceId);
    }

    /**
     * Extract the writeLease, updateOrWrite & updateTimeout from the URI. If atrribute is missing
     * sets the default.
     */
    private void initWritingAttributes(ImmutableEndpoint endpoint) {
        Properties params = endpoint.getEndpointURI().getParams();
        if (params != null) {
            try {
                String writeLeaseStr = (String) params.get(ENDPOINT_PARAM_WRITE_LEASE);
                if (writeLeaseStr != null) {
                    writeLease = Long.valueOf(writeLeaseStr);
                }
                String updateOrWriteStr = (String) params.get(ENDPOINT_PARAM_UPDATE_OR_WRITE);
                if (updateOrWriteStr != null) {
                    updateOrWrite = Boolean.valueOf(updateOrWriteStr);
                }
                String updateTimeoutStr = (String) params.get(ENDPOINT_PARAM_UPDATE_TIMEOUT);
                if (updateTimeoutStr != null) {
                    updateTimeout = Long.valueOf(updateTimeoutStr);
                }
            } catch (NumberFormatException e) {
                throw new MuleRuntimeException(CoreMessages.failedToCreateConnectorFromUri(endpoint.getEndpointURI()), e);
            }
        }
    }

    protected void doDispose() {
    }

    protected void doDispatch(MuleEvent event) throws Exception {
        doSend(event);
    }

    protected MuleMessage doSend(MuleEvent event) throws Exception {

        Object payload = event.getMessage().getPayload();

        if (payload != null) {
            int updateModifiers = updateOrWrite ? UpdateModifiers.UPDATE_OR_WRITE : UpdateModifiers.WRITE_ONLY;
            if (payload instanceof Object[]) {
                gigaSpace.writeMultiple((Object[]) payload, writeLease, updateModifiers);
            } else {
                gigaSpace.write(payload, writeLease, updateTimeout, updateModifiers);
            }
        }
        return null;
    }


    protected void doConnect() throws Exception {
    }

    protected void doDisconnect() throws Exception {
    }

    protected MuleMessage doReceive(long timeout) throws Exception {
        return null;
    }
}
