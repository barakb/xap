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

import org.openspaces.core.space.mode.SpaceModeContextLoader;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * A bean definition builder for {@link SpaceModeContextLoader}.
 *
 * @author kimchy
 */
public class ContextLoaderBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

    private static final String GIGA_SPACE = "giga-space";

    protected Class getBeanClass(Element element) {
        return SpaceModeContextLoader.class;
    }

    protected boolean isEligibleAttribute(String attributeName) {
        return super.isEligibleAttribute(attributeName) && !GIGA_SPACE.equals(attributeName);
    }

    protected void postProcess(BeanDefinitionBuilder beanDefinition, Element element) {
        String gigaSpace = element.getAttribute(GIGA_SPACE);
        if (StringUtils.hasLength(gigaSpace)) {
            beanDefinition.addPropertyReference("gigaSpace", gigaSpace);
        }
    }
}
