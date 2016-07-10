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
 * @author kimchy
 */
public class AnnotationEventAdapterBeanDefinitionParser extends AbstractResultEventAdapterBeanDefinitionParser {

    public static final String DELEGATE = "delegate";

    protected Class<AnnotationEventAdapterFactoryBean> getBeanClass(Element element) {
        return AnnotationEventAdapterFactoryBean.class;
    }

    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);
        Element delegateEle = DomUtils.getChildElementByTagName(element, DELEGATE);
        builder.addPropertyValue("delegate", parserContext.getDelegate().parsePropertyValue(delegateEle,
                builder.getRawBeanDefinition(), "delegate"));
    }
}
