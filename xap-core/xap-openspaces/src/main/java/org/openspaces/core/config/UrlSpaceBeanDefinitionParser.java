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

import org.openspaces.core.space.UrlSpaceFactoryBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * A bean definition builder for {@link UrlSpaceFactoryBean}.
 *
 * @author kimchy
 */
public class UrlSpaceBeanDefinitionParser extends AbstractSpaceBeanDefinitionParser {

    public static final String PARAMETERS = "parameters";
    public static final String URL_PROPERTIES = "url-properties";

    @Override
    protected Class<UrlSpaceFactoryBean> getBeanClass(Element element) {
        return UrlSpaceFactoryBean.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);

        Element parametersEle = DomUtils.getChildElementByTagName(element, PARAMETERS);
        if (parametersEle != null) {
            Object parameters = parserContext.getDelegate().parsePropertyValue(parametersEle,
                    builder.getRawBeanDefinition(), "parameters");
            builder.addPropertyValue("parameters", parameters);
        }
        Element urlPropertiesEle = DomUtils.getChildElementByTagName(element, URL_PROPERTIES);
        if (urlPropertiesEle != null) {
            Object properties = parserContext.getDelegate().parsePropertyValue(urlPropertiesEle,
                    builder.getRawBeanDefinition(), "urlProperties");
            builder.addPropertyValue("urlProperties", properties);
        }

        parseServerComponents(element, parserContext, builder);
    }
}
