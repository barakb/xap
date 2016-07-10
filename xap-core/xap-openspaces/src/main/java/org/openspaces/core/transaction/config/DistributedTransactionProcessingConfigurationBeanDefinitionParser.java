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

package org.openspaces.core.transaction.config;

import org.openspaces.core.transaction.DistributedTransactionProcessingConfigurationFactoryBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * A bean definition parser for distributed transaction support configuration for mirror component.
 *
 * @author idan
 * @since 8.0.4
 */
public class DistributedTransactionProcessingConfigurationBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

    final private static String WAIT_FOR_OPERATIONS = "dist-tx-wait-for-opers";
    final private static String WAIT_TIMEOUT = "dist-tx-wait-timeout-millis";
    final private static String MONITOR_MEMORY = "monitor-pending-opers-memory";

    @Override
    protected Class<?> getBeanClass(Element element) {
        return DistributedTransactionProcessingConfigurationFactoryBean.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        final String distributedTransactionWaitTimeout = element.getAttribute(WAIT_TIMEOUT);
        if (StringUtils.hasLength(distributedTransactionWaitTimeout))
            builder.addPropertyValue("distributedTransactionWaitTimeout", distributedTransactionWaitTimeout);

        final String distributedTransactionWaitForOperations = element.getAttribute(WAIT_FOR_OPERATIONS);
        if (StringUtils.hasLength(distributedTransactionWaitTimeout))
            builder.addPropertyValue("distributedTransactionWaitForOperations", distributedTransactionWaitForOperations);

        final String monitorPendingOperationsMemory = element.getAttribute(MONITOR_MEMORY);
        if (StringUtils.hasLength(monitorPendingOperationsMemory))
            builder.addPropertyValue("monitorPendingOperationsMemory", monitorPendingOperationsMemory);

    }


}
