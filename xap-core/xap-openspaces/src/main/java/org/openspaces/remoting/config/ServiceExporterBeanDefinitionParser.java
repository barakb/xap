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


package org.openspaces.remoting.config;

import org.openspaces.remoting.ServiceRef;
import org.openspaces.remoting.SpaceRemotingServiceExporter;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import java.util.List;

/**
 * @author kimchy
 */
public class ServiceExporterBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    private static final String SERVICE = "service";

    private static final String ASPECT = "aspect";

    protected Class<SpaceRemotingServiceExporter> getBeanClass(Element element) {
        return SpaceRemotingServiceExporter.class;
    }

    @SuppressWarnings("unchecked")
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);

        String templateLookupName = element.getAttribute("template-lookup-name");
        if (StringUtils.hasLength(templateLookupName)) {
            builder.addPropertyValue("templateLookupName", templateLookupName);
        }

        Element aspectEle = DomUtils.getChildElementByTagName(element, ASPECT);
        if (aspectEle != null) {
            builder.addPropertyValue("serviceExecutionAspect", parserContext.getDelegate().parsePropertyValue(
                    aspectEle, builder.getRawBeanDefinition(), "serviceExecutionAspect"));
        }

        List<Element> serviceElements = DomUtils.getChildElementsByTagName(element, SERVICE);
        ManagedList list = new ManagedList(serviceElements.size());
        for (Element ele : serviceElements) {
            String ref = ele.getAttribute("ref");
            if (ref != null) {
                list.add(new ServiceRef(ref));
            } else {
                list.add(parserContext.getDelegate().parsePropertyValue(ele, builder.getRawBeanDefinition(), null));
            }
        }
        builder.addPropertyValue("services", list);
    }
}