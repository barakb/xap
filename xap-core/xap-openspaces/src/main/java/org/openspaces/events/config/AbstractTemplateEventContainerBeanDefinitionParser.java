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
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * @author Itai Frenkel
 */
public class AbstractTemplateEventContainerBeanDefinitionParser extends AbstractTxEventContainerBeanDefinitionParser {

    private static final String TEMPLATE = "template";

    private static final String SQL_QUERY = "sql-query";

    private static final String DYNAMIC_TEMPLATE = "dynamic-template";

    protected boolean isSupportsDynamicTemplate() {
        return true;
    }

    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        super.doParse(element, parserContext, builder);

        Element dynamicTemplateEle = DomUtils.getChildElementByTagName(element, DYNAMIC_TEMPLATE);
        if (isSupportsDynamicTemplate() && dynamicTemplateEle != null) {
            builder.addPropertyValue("dynamicTemplate", parserContext.getDelegate().parsePropertyValue(dynamicTemplateEle,
                    builder.getRawBeanDefinition(), "dynamicTemplate"));
        } else {

            Element templateEle = DomUtils.getChildElementByTagName(element, TEMPLATE);
            if (templateEle != null) {
                Object template = parserContext.getDelegate().parsePropertyValue(templateEle,
                        builder.getRawBeanDefinition(), "template");
                builder.addPropertyValue("template", template);
            }

            Element sqlQueryEle = DomUtils.getChildElementByTagName(element, SQL_QUERY);
            if (sqlQueryEle != null) {
                builder.addPropertyValue("template", parserContext.getDelegate().parsePropertySubElement(sqlQueryEle,
                        builder.getRawBeanDefinition(), null));
            }
        }
    }
}
