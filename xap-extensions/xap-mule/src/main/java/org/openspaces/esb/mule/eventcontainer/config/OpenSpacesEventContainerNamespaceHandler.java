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


package org.openspaces.esb.mule.eventcontainer.config;

import org.mule.config.spring.MuleHierarchicalBeanDefinitionParserDelegate;
import org.mule.config.spring.factories.InboundEndpointFactoryBean;
import org.mule.config.spring.factories.OutboundEndpointFactoryBean;
import org.mule.config.spring.handlers.AbstractMuleNamespaceHandler;
import org.mule.config.spring.parsers.generic.MuleOrphanDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.TransportEndpointDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.TransportGlobalEndpointDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.support.AddressedEndpointDefinitionParser;
import org.mule.endpoint.URIBuilder;
import org.openspaces.esb.mule.eventcontainer.OpenSpacesConnector;

/**
 * A namespace handler for <code>OpenSpaces</code> namespace.
 *
 * @author yitzhaki
 */
public class OpenSpacesEventContainerNamespaceHandler extends AbstractMuleNamespaceHandler {

    public void init() {
//        registerStandardTransportEndpoints(OpenSpacesConnector.OS_EVENT_CONTAINER, new String[]{});

        TransportGlobalEndpointDefinitionParser transportGlobalEndpointDefinitionParser = new TransportGlobalEndpointDefinitionParser(OpenSpacesConnector.OS_EVENT_CONTAINER, AddressedEndpointDefinitionParser.PROTOCOL, URIBuilder.PATH_ATTRIBUTES, new String[]{});
        transportGlobalEndpointDefinitionParser.addBeanFlag(MuleHierarchicalBeanDefinitionParserDelegate.MULE_FORCE_RECURSE);
        registerBeanDefinitionParser("endpoint", transportGlobalEndpointDefinitionParser);

        TransportEndpointDefinitionParser inboundParser = new TransportEndpointDefinitionParser(OpenSpacesConnector.OS_EVENT_CONTAINER, AddressedEndpointDefinitionParser.PROTOCOL, InboundEndpointFactoryBean.class, new String[]{}, new String[]{});
        inboundParser.addBeanFlag(MuleHierarchicalBeanDefinitionParserDelegate.MULE_FORCE_RECURSE);
        registerBeanDefinitionParser("inbound-endpoint", inboundParser);

        TransportEndpointDefinitionParser outboundParser = new TransportEndpointDefinitionParser(OpenSpacesConnector.OS_EVENT_CONTAINER, AddressedEndpointDefinitionParser.PROTOCOL, OutboundEndpointFactoryBean.class, new String[]{"giga-space"}, new String[]{});
        outboundParser.addBeanFlag(MuleHierarchicalBeanDefinitionParserDelegate.MULE_FORCE_RECURSE);
        outboundParser.addAlias("giga-space", URIBuilder.PATH);
        registerBeanDefinitionParser("outbound-endpoint", outboundParser);

        registerBeanDefinitionParser("connector", new MuleOrphanDefinitionParser(OpenSpacesConnector.class, true));
    }
}
