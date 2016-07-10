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

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceDynamicProperties;
import com.gigaspaces.annotation.pojo.SpaceExclude;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceIndex;
import com.gigaspaces.annotation.pojo.SpacePersist;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.document.DocumentProperties;

import org.openspaces.core.util.ThreadLocalMarshaller;

import java.io.IOException;

/**
 * An internal queue entry holding the endpoint address and the queue payload. The payload can be
 * serialized when written to space, to avoid class loading issues on space side.
 *
 * @author anna
 */
@SpaceClass(replicate = true, fifoSupport = FifoSupport.OPERATION)
public class OpenSpacesQueueObject {

    public static final String RESPONSE_TIMEOUT_PROPERTY = "OS_QUEUE_RESPONSE_TIMEOUT";

    private String id;
    private String endpointURI;

    private Object internalPayload;

    private DocumentProperties payloadMetaData = new DocumentProperties();

    private boolean isPersistent = true;

    // kept as a separate field and not in the meta data map for more efficient queries
    private String correlationID;

    public OpenSpacesQueueObject() {
    }

    @SpaceRouting
    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setEndpointURI(String endpointURI) {
        this.endpointURI = endpointURI;
    }

    @SpaceIndex
    public String getEndpointURI() {
        return endpointURI;
    }

    @SpaceExclude
    public Object getPayload() throws IOException, ClassNotFoundException {
        if (internalPayload == null)
            return null;
        return ThreadLocalMarshaller.objectFromByteBuffer((byte[]) internalPayload);
    }

    public void setPayload(Object payload) throws IOException {
        this.internalPayload = ThreadLocalMarshaller.objectToByteBuffer(payload);
    }

    /**
     * For internal usage only, to get the payload use {@link #getPayload()} method.
     *
     * @return internalPayload
     */
    public Object getInternalPayload() {
        return internalPayload;
    }

    /**
     * For internal usage only, to set the payload use {@link #setPayload(Object payload)} method.
     */
    public void setInternalPayload(Object payload) {
        this.internalPayload = payload;
    }

    @SpaceDynamicProperties
    public DocumentProperties getPayloadMetaData() {
        return payloadMetaData;
    }

    public void setPayloadMetaData(DocumentProperties payloadMetaData) {
        this.payloadMetaData = payloadMetaData;
    }

    public String getCorrelationID() {
        return correlationID;
    }

    public void setCorrelationID(String correlationID) {
        this.correlationID = correlationID;
    }

    @SpacePersist
    public boolean getPersistent() {
        return isPersistent;
    }

    public void setPersistent(boolean isPersistent) {
        this.isPersistent = isPersistent;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [endpointURI=" + endpointURI + ", correlationId=" + correlationID + ", internalPayload=" + internalPayload
                + ", payloadMetaData=" + payloadMetaData + ", isPersistent=" + isPersistent + "]";
    }

}
