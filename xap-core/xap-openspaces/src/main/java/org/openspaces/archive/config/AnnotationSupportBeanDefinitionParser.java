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


package org.openspaces.archive.config;

import org.openspaces.archive.ArchivePollingAnnotationPostProcessor;
import org.openspaces.events.support.EventContainersBus;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * @author Itai Frenkel
 * @see ArchiveNamespaceHandler
 * @since 9.1.1
 */
public class AnnotationSupportBeanDefinitionParser implements BeanDefinitionParser {

    public static final String SECONDARY_EVENT_CONTAINER_BUS_BEAN_NAME = "internal-archiveContainerBus";

    public BeanDefinition parse(Element element, ParserContext parserContext) {

        {
            BeanDefinition bd = new RootBeanDefinition(EventContainersBus.class);
            bd.setLazyInit(true);
            bd.setPrimary(false); // prefer <os-events:annotation-support> which creates internal-eventsContainerBus
            BeanComponentDefinition bcd = new BeanComponentDefinition(bd, SECONDARY_EVENT_CONTAINER_BUS_BEAN_NAME);
            parserContext.registerBeanComponent(bcd);
        }

        {
            RootBeanDefinition bd = new RootBeanDefinition(ArchivePollingAnnotationPostProcessor.class);
            BeanComponentDefinition bcd = new BeanComponentDefinition(bd, "internal-archivePollingContainerAnnotationPostProcessor");
            parserContext.registerBeanComponent(bcd);
        }

        return null;
    }
}
