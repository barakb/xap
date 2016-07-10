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


package org.openspaces.esb.mule.queue.config;

import org.mule.config.spring.MuleHierarchicalBeanDefinitionParserDelegate;
import org.mule.config.spring.factories.InboundEndpointFactoryBean;
import org.mule.config.spring.factories.OutboundEndpointFactoryBean;
import org.mule.config.spring.handlers.AbstractMuleNamespaceHandler;
import org.mule.config.spring.parsers.generic.MuleOrphanDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.TransportEndpointDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.TransportGlobalEndpointDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.support.AddressedEndpointDefinitionParser;
import org.mule.endpoint.URIBuilder;
import org.openspaces.esb.mule.queue.OpenSpacesQueueConnector;

/**
 * The OpenSpaces queue namespace support.
 *
 * @author kimchy
 */
public class OpenSpacesQueueNamespaceHandler extends AbstractMuleNamespaceHandler {

    public void init() {
        registerStandardTransportEndpoints(OpenSpacesQueueConnector.OS_QUEUE, URIBuilder.PATH_ATTRIBUTES);

        TransportGlobalEndpointDefinitionParser transportGlobalEndpointDefinitionParser = new TransportGlobalEndpointDefinitionParser(OpenSpacesQueueConnector.OS_QUEUE, AddressedEndpointDefinitionParser.PROTOCOL, URIBuilder.PATH_ATTRIBUTES, new String[]{});
        transportGlobalEndpointDefinitionParser.addBeanFlag(MuleHierarchicalBeanDefinitionParserDelegate.MULE_FORCE_RECURSE);
        registerBeanDefinitionParser("endpoint", transportGlobalEndpointDefinitionParser);

        TransportEndpointDefinitionParser endpointDefinitionParser = new TransportEndpointDefinitionParser(OpenSpacesQueueConnector.OS_QUEUE, AddressedEndpointDefinitionParser.PROTOCOL, InboundEndpointFactoryBean.class, URIBuilder.PATH_ATTRIBUTES, new String[]{});
        endpointDefinitionParser.addBeanFlag(MuleHierarchicalBeanDefinitionParserDelegate.MULE_FORCE_RECURSE);
        registerBeanDefinitionParser("inbound-endpoint", endpointDefinitionParser);
        TransportEndpointDefinitionParser transportEndpointDefinitionParser = new TransportEndpointDefinitionParser(OpenSpacesQueueConnector.OS_QUEUE, AddressedEndpointDefinitionParser.PROTOCOL, OutboundEndpointFactoryBean.class, URIBuilder.PATH_ATTRIBUTES, new String[]{});
        transportEndpointDefinitionParser.addBeanFlag(MuleHierarchicalBeanDefinitionParserDelegate.MULE_FORCE_RECURSE);
        registerBeanDefinitionParser("outbound-endpoint", transportEndpointDefinitionParser);

        registerBeanDefinitionParser("connector", new MuleOrphanDefinitionParser(OpenSpacesQueueConnector.class, true));
    }

}
