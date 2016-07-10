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
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * @author kimchy
 */
public abstract class AbstractResultEventAdapterBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    private static final String WRITE_LEASE = "write-lease";

    private static final String UPDATE_OR_WRITE = "update-or-write";

    private static final String UPDATE_TIMEOUT = "update-timeout";

    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);

        String writeLease = element.getAttribute(WRITE_LEASE);
        if (StringUtils.hasLength(writeLease)) {
            builder.addPropertyValue("writeLease", Long.valueOf(writeLease));
        }

        String updateOrWrite = element.getAttribute(UPDATE_OR_WRITE);
        if (StringUtils.hasLength(updateOrWrite)) {
            builder.addPropertyValue("updateOrWrite", Boolean.valueOf(updateOrWrite));
        }

        String updateTimeout = element.getAttribute(UPDATE_TIMEOUT);
        if (StringUtils.hasLength(updateTimeout)) {
            builder.addPropertyValue("updateTimeout", Long.valueOf(updateTimeout));
        }

        String scope = element.getAttribute("scope");
        if (StringUtils.hasLength(scope)) {
            builder.setScope(scope);
        }
    }
}
