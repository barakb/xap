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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * @author kimchy
 */
public abstract class AbstractTxEventContainerBeanDefinitionParser extends AbstractEventContainerBeanDefinitionParser {

    private static final String TX_MANAGER = "tx-manager";

    private static final String TX_NAME = "tx-name";

    private static final String TX_TIMEOUT = "tx-timeout";

    private static final String TX_ISOLATION = "tx-isolation";

    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);

        Element txElement = DomUtils.getChildElementByTagName(element, "tx-support");
        if (txElement != null) {
            String txManager = txElement.getAttribute(TX_MANAGER);
            if (StringUtils.hasLength(txManager)) {
                builder.addPropertyReference("transactionManager", txManager);
            }

            String txName = txElement.getAttribute(TX_NAME);
            if (StringUtils.hasLength(txName)) {
                builder.addPropertyValue("transactionName", txName);
            }

            String txTimeout = txElement.getAttribute(TX_TIMEOUT);
            if (StringUtils.hasLength(txTimeout)) {
                builder.addPropertyValue("transactionTimeout", txTimeout);
            }

            String txIsolation = txElement.getAttribute(TX_ISOLATION);
            if (StringUtils.hasLength(txIsolation)) {
                builder.addPropertyValue("transactionIsolationLevelName", DefaultTransactionDefinition.PREFIX_ISOLATION
                        + txIsolation);
            }
        }
    }
}