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
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import java.util.Properties;

/**
 * @author yohana
 * @since 10.1.0
 */
public class RestBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {
    private static final String GIGA_SPACE = "giga-space";
    private static final String PORT = "port";
    private static final String SPACE_NAME = "space-name";
    private static final String GROUPS = "lookup-groups";
    private static final String LOCATORS = "lookup-locators";
    private static final String PROPERTIES = "properties";

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);
        String gigaSpace = element.getAttribute(GIGA_SPACE);
        if (StringUtils.hasLength(gigaSpace)) {
            builder.addPropertyReference("gigaSpace", gigaSpace);
        }


        String spaceName = element.getAttribute(SPACE_NAME);
        if (StringUtils.hasLength(spaceName)) {
            builder.addPropertyValue("spaceName", spaceName);
        }

        String groups = element.getAttribute(GROUPS);
        if (StringUtils.hasLength(groups)) {
            builder.addPropertyValue("groups", groups);
        }

        String locators = element.getAttribute(LOCATORS);
        if (StringUtils.hasLength(locators)) {
            builder.addPropertyValue("locators", locators);
        }

        String port = element.getAttribute(PORT);
        if (StringUtils.hasLength(port)) {
            builder.addPropertyValue("port", port);
        }

        Element propertiesEle = DomUtils.getChildElementByTagName(element, PROPERTIES);
        if (propertiesEle != null) {
            Element propsEle = DomUtils.getChildElementByTagName(propertiesEle, "props");
            if (propsEle != null) {
                Properties props = parserContext.getDelegate().parsePropsElement(propsEle);
                builder.addPropertyValue("properties", props);
            }
        }
    }

    @Override
    protected String getBeanClassName(Element element) {
        return "org.openspaces.core.space.RestBean";
    }
}
