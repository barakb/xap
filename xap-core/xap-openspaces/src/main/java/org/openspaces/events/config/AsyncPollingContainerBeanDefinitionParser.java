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


package org.openspaces.events.config;

import org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * @author kimchy
 */
public class AsyncPollingContainerBeanDefinitionParser extends AbstractTemplateEventContainerBeanDefinitionParser {

    private static final String ASYNC_OPERATION_HANDLER = "async-operation-handler";

    private static final String RECEIVE_TIMEOUT = "receive-timeout";

    private static final String CONCURRENT_CONSUMERS = "concurrent-consumers";

    private static final String PERFORM_SNAPSHOT = "perform-snapshot";

    protected Class<SimpleAsyncPollingEventListenerContainer> getBeanClass(Element element) {
        return SimpleAsyncPollingEventListenerContainer.class;
    }

    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        super.doParse(element, parserContext, builder);

        Element asyncOperationHandlerEle = DomUtils.getChildElementByTagName(element, ASYNC_OPERATION_HANDLER);
        if (asyncOperationHandlerEle != null) {
            builder.addPropertyValue("asyncOperationHandler",
                    parserContext.getDelegate().parsePropertyValue(asyncOperationHandlerEle, builder.getRawBeanDefinition(), "asyncOperationHandler"));
        }

        String receiveTimeout = element.getAttribute(RECEIVE_TIMEOUT);
        if (StringUtils.hasLength(receiveTimeout)) {
            builder.addPropertyValue("receiveTimeout", receiveTimeout);
        }

        String concurrentConsumers = element.getAttribute(CONCURRENT_CONSUMERS);
        if (StringUtils.hasLength(concurrentConsumers)) {
            builder.addPropertyValue("concurrentConsumers", concurrentConsumers);
        }

        String performSnapshot = element.getAttribute(PERFORM_SNAPSHOT);
        if (StringUtils.hasLength(performSnapshot)) {
            builder.addPropertyValue("performSnapshot", performSnapshot);
        }
    }
}