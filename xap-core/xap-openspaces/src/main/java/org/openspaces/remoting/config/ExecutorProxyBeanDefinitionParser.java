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

import org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * A bean definition builder for {@link org.openspaces.remoting.ExecutorSpaceRemotingProxyFactoryBean}.
 *
 * @author kimchy
 */
public class ExecutorProxyBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    private static final String GIGA_SPACE = "giga-space";

    private static final String INTERFACE = "interface";

    private static final String BROADCAST = "broadcast";

    private static final String RETURN_FIRST_RESULT = "return-first-result";

    private static final String ROUTING_HANDLER = "routing-handler";

    private static final String META_ARGUMENT_HANDLER = "meta-arguments-handler";

    private static final String RESULT_REDUCER = "result-reducer";

    private static final String ASPECT = "aspect";

    private static final String TIMEOUT = "timeout";

    protected Class<ExecutorSpaceRemotingProxyFactoryBean> getBeanClass(Element element) {
        return ExecutorSpaceRemotingProxyFactoryBean.class;
    }

    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);

        String gigaSpace = element.getAttribute(GIGA_SPACE);
        builder.addPropertyReference("gigaSpace", gigaSpace);

        String inter = element.getAttribute(INTERFACE);
        builder.addPropertyValue("serviceInterface", inter);

        String broadcast = element.getAttribute(BROADCAST);
        if (StringUtils.hasLength(broadcast)) {
            builder.addPropertyValue("broadcast", broadcast);
        }

        String timeout = element.getAttribute(TIMEOUT);
        if (StringUtils.hasLength(timeout)) {
            builder.addPropertyValue("timeout", timeout);
        }

        String returnFirstResult = element.getAttribute(RETURN_FIRST_RESULT);
        if (StringUtils.hasLength(returnFirstResult)) {
            builder.addPropertyValue("returnFirstResult", returnFirstResult);
        }

        Element routingHandlerEle = DomUtils.getChildElementByTagName(element, ROUTING_HANDLER);
        if (routingHandlerEle != null) {
            builder.addPropertyValue("remoteRoutingHandler", parserContext.getDelegate().parsePropertyValue(
                    routingHandlerEle, builder.getRawBeanDefinition(), "remoteRoutingHandler"));
        }

        Element aspectEle = DomUtils.getChildElementByTagName(element, ASPECT);
        if (aspectEle != null) {
            builder.addPropertyValue("remoteInvocationAspect", parserContext.getDelegate().parsePropertyValue(
                    aspectEle, builder.getRawBeanDefinition(), "remoteInvocationAspect"));
        }

        Element resultReducerEle = DomUtils.getChildElementByTagName(element, RESULT_REDUCER);
        if (resultReducerEle != null) {
            builder.addPropertyValue("remoteResultReducer", parserContext.getDelegate().parsePropertyValue(
                    resultReducerEle, builder.getRawBeanDefinition(), "remoteResultReducer"));
        }

        Element metaArguemntsHandlerEle = DomUtils.getChildElementByTagName(element, META_ARGUMENT_HANDLER);
        if (metaArguemntsHandlerEle != null) {
            builder.addPropertyValue("metaArgumentsHandler", parserContext.getDelegate().parsePropertyValue(
                    metaArguemntsHandlerEle, builder.getRawBeanDefinition(), "metaArgumentsHandler"));
        }
    }
}