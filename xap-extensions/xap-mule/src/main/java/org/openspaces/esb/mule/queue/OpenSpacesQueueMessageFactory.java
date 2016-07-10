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

import org.mule.DefaultMuleMessage;
import org.mule.api.MuleContext;
import org.mule.api.transport.MessageTypeNotSupportedException;
import org.mule.transport.AbstractMuleMessageFactory;

public class OpenSpacesQueueMessageFactory extends AbstractMuleMessageFactory {

    public OpenSpacesQueueMessageFactory() {
    }

    public OpenSpacesQueueMessageFactory(MuleContext ctx) {
        super(ctx);
    }

    @Override
    protected Object extractPayload(Object transportMessage, String encoding) throws Exception {
        if (transportMessage == null) {
            throw new MessageTypeNotSupportedException(transportMessage, getClass());
        }

        if (transportMessage instanceof OpenSpacesQueueObject) {
            return ((OpenSpacesQueueObject) transportMessage).getPayload();
        }
        // handle previous versions when the payload itself was passed to this method
        else {
            return transportMessage;
        }

    }

    @Override
    protected Class<?>[] getSupportedTransportMessageTypes() {
        return new Class[]{Object.class};
    }

    @Override
    protected void addProperties(DefaultMuleMessage message, Object transportMessage) throws Exception {
        super.addProperties(message, transportMessage);

        if (transportMessage instanceof OpenSpacesQueueObject) {
            OpenSpacesQueueObject queueObject = (OpenSpacesQueueObject) transportMessage;

            if (queueObject.getPayloadMetaData() != null)
                message.addProperties(queueObject.getPayloadMetaData());

            String correlationId = queueObject.getCorrelationID();

            if (correlationId != null)
                message.setCorrelationId(correlationId);

        }
    }

}
