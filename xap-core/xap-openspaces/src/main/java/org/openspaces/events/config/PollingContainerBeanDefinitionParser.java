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

import org.openspaces.events.polling.SimplePollingEventListenerContainer;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * @author kimchy
 */
public class PollingContainerBeanDefinitionParser extends AbstractTemplateEventContainerBeanDefinitionParser {

    private static final String RECEIVE_OPERATION_HANDLER = "receive-operation-handler";

    private static final String TRIGGER_OPERATION_HANDLER = "trigger-operation-handler";

    private static final String RECEIVE_TIMEOUT = "receive-timeout";

    private static final String RECOVERY_INTERVAL = "recovery-interval";

    private static final String CONCURRENT_CONSUMERS = "concurrent-consumers";

    private static final String MAX_CONCURRENT_CONSUMERS = "max-concurrent-consumers";

    private static final String IDLE_TASK_EXECUTION_LIMIT = "idle-task-execution-limit";

    private static final String PERFORM_SNAPSHOT = "perform-snapshot";

    private static final String PASS_ARRAY_AS_IS = "pass-array-as-is";

    protected Class<SimplePollingEventListenerContainer> getBeanClass(Element element) {
        return SimplePollingEventListenerContainer.class;
    }

    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        super.doParse(element, parserContext, builder);

        Element receiveOperationHandlerEle = DomUtils.getChildElementByTagName(element, RECEIVE_OPERATION_HANDLER);
        if (receiveOperationHandlerEle != null) {
            builder.addPropertyValue("receiveOperationHandler",
                    parserContext.getDelegate().parsePropertyValue(receiveOperationHandlerEle, builder.getRawBeanDefinition(), "receiveOperationHandler"));
        }

        Element triggerOperationHandlerEle = DomUtils.getChildElementByTagName(element, TRIGGER_OPERATION_HANDLER);
        if (triggerOperationHandlerEle != null) {
            builder.addPropertyValue("triggerOperationHandler",
                    parserContext.getDelegate().parsePropertyValue(triggerOperationHandlerEle, builder.getRawBeanDefinition(), "triggerOperationHandler"));
        }

        String receiveTimeout = element.getAttribute(RECEIVE_TIMEOUT);
        if (StringUtils.hasLength(receiveTimeout)) {
            builder.addPropertyValue("receiveTimeout", receiveTimeout);
        }

        String recoveryInterval = element.getAttribute(RECOVERY_INTERVAL);
        if (StringUtils.hasLength(recoveryInterval)) {
            builder.addPropertyValue("recoveryInterval", recoveryInterval);
        }

        String concurrentConsumers = element.getAttribute(CONCURRENT_CONSUMERS);
        if (StringUtils.hasLength(concurrentConsumers)) {
            builder.addPropertyValue("concurrentConsumers", concurrentConsumers);
        }

        String maxConcurrentConsumers = element.getAttribute(MAX_CONCURRENT_CONSUMERS);
        if (StringUtils.hasLength(maxConcurrentConsumers)) {
            builder.addPropertyValue("maxConcurrentConsumers", maxConcurrentConsumers);
        }

        String idleTaskExecutionLimit = element.getAttribute(IDLE_TASK_EXECUTION_LIMIT);
        if (StringUtils.hasLength(idleTaskExecutionLimit)) {
            builder.addPropertyValue("idleTaskExecutionLimit", idleTaskExecutionLimit);
        }

        String performSnapshot = element.getAttribute(PERFORM_SNAPSHOT);
        if (StringUtils.hasLength(performSnapshot)) {
            builder.addPropertyValue("performSnapshot", performSnapshot);
        }

        String passArrayAsIs = element.getAttribute(PASS_ARRAY_AS_IS);
        if (StringUtils.hasLength(passArrayAsIs)) {
            builder.addPropertyValue("passArrayAsIs", passArrayAsIs);
        }
    }
}
