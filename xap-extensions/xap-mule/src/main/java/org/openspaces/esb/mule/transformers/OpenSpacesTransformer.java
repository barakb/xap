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

package org.openspaces.esb.mule.transformers;

import org.mule.api.MuleMessage;
import org.mule.api.transformer.TransformerException;
import org.mule.transformer.AbstractMessageTransformer;
import org.openspaces.esb.mule.message.MessageHeader;

import java.util.Iterator;

/**
 * This class copies the metadata from UMOMessage to the transformed object. it assumes that the
 * return object is the payload that contained in the UMOMessage.
 *
 * Note: In case that the return object isn't the payload that contained in the UMOMessage you
 * should override the {@link org.openspaces.esb.mule.transformers.OpenSpacesTransformer#getResultPayload(org.mule.api.MuleMessage,
 * String)} method and return the new transformered object.
 *
 * @author yitzhaki
 */
public class OpenSpacesTransformer extends AbstractMessageTransformer {

    @Override
    public Object transformMessage(MuleMessage message, String outputEncoding) throws TransformerException {
        Object result = getResultPayload(message, outputEncoding);
        copyMetaData(message, result);
        return result;
    }

    /**
     * Returns the origin UMOMessage payload.
     */
    protected Object getResultPayload(MuleMessage message, String outputEncoding) {
        return message.getPayload();
    }

    /**
     * Copies the metadata from the UMOMessage to the result object.
     */
    protected void copyMetaData(MuleMessage message, Object result) {
        if (result instanceof MessageHeader) {
            Iterator names = message.getPropertyNames().iterator();
            while (names.hasNext()) {
                String name = (String) names.next();
                Object value = message.getProperty(name);
                ((MessageHeader) result).setProperty(name, value);
            }
        }
    }

}
