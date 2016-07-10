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

import org.mule.DefaultMuleMessage;
import org.mule.api.MuleContext;
import org.mule.api.transport.MessageTypeNotSupportedException;
import org.mule.transport.AbstractMuleMessageFactory;
import org.mule.util.UUID;
import org.openspaces.esb.mule.message.MessageHeader;
import org.openspaces.esb.mule.message.UniqueIdMessageHeader;

import java.util.Iterator;

public class OpenSpacesMessageFactory extends AbstractMuleMessageFactory {

    public OpenSpacesMessageFactory() {

    }

    public OpenSpacesMessageFactory(MuleContext ctx) {
        super(ctx);
    }

    @Override
    protected Object extractPayload(Object transportMessage, String encoding) throws Exception {
        if (transportMessage == null) {
            throw new MessageTypeNotSupportedException(transportMessage, getClass());
        }
        return transportMessage;
    }

    @Override
    protected Class<?>[] getSupportedTransportMessageTypes() {
        return new Class[]{Object.class};
    }

    @Override
    protected void addProperties(DefaultMuleMessage message, Object transportMessage) throws Exception {
        super.addProperties(message, transportMessage);

        if (transportMessage instanceof MessageHeader) {
            Iterator keys = ((MessageHeader) transportMessage).getProperties().keySet().iterator();
            while (keys.hasNext()) {
                String key = (String) keys.next();
                Object value = ((MessageHeader) transportMessage).getProperty(key);
                if (value != null) {
                    message.setProperty(key, value);
                }
            }
        }
        if (message.getProperty(UniqueIdMessageHeader.UNIQUE_ID) == null) {
            message.setProperty(UniqueIdMessageHeader.UNIQUE_ID, UUID.getUUID());
        }
    }

}
