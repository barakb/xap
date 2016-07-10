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

package org.openspaces.core.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;


/**
 * @author Kobi
 * @since 10.2.0
 */
public class AttributeStoreBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

    private static final String STORE_HANDLER = "store-handler";

    @Override
    protected Class<AttributeStoreFactoryBean> getBeanClass(Element element) {
        return AttributeStoreFactoryBean.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        final String attributeStoreHandler = element.getAttribute(STORE_HANDLER);
        if (StringUtils.hasText(attributeStoreHandler))
            builder.addPropertyReference("storeHandler", attributeStoreHandler);
    }

}
