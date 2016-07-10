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
 * @author idan
 * @since 9.1
 */
public class CustomCachePolicyBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

    private static final String SIZE_PROPERTY = "size";
    private static final String INITIAL_LOAD_PERCENTAGE_PROPERTY = "initial-load-percentage";
    private static final String SPACE_EVICTION_STRATEGY_PROPERTY = "space-eviction-strategy";

    @Override
    protected Class<CustomCachePolicyFactoryBean> getBeanClass(Element element) {
        return CustomCachePolicyFactoryBean.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        final String size = element.getAttribute(SIZE_PROPERTY);
        if (StringUtils.hasText(size))
            builder.addPropertyValue("size", size);

        final String initialLoadPercentage = element.getAttribute(INITIAL_LOAD_PERCENTAGE_PROPERTY);
        if (StringUtils.hasText(initialLoadPercentage)) {
            builder.addPropertyValue("initialLoadPercentage", initialLoadPercentage);
        }

        final String spaceEvictionStrategyBeanName = element.getAttribute(SPACE_EVICTION_STRATEGY_PROPERTY);
        if (!StringUtils.hasText(spaceEvictionStrategyBeanName))
            throw new IllegalArgumentException("A reference to a space eviction strategy bean must be specified using the 'ref' attribute");
        builder.addPropertyReference("spaceEvictionStrategy", spaceEvictionStrategyBeanName);
    }

}
