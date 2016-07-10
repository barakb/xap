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

package org.openspaces.persistency.cassandra.archive;

import org.openspaces.archive.config.ArchiveNamespaceHandler;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Parses "<os-archive:cassandra-archive-handler>
 *
 * @author itaif
 * @see ArchiveNamespaceHandler
 * @since 9.1.1
 */
public class CassandraArchiveOperationHandlerBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    private static final String GIGA_SPACE = "giga-space";
    private static final String CASSANDRA_KEYSPACE = "keyspace";
    private static final String CASSANDRA_HOSTS = "hosts";
    private static final String CASSANDRA_PORT = "port";
    private static final String CASSANDRA_CONSISTENCY = "write-consistency";

    @Override
    protected Class<CassandraArchiveOperationHandler> getBeanClass(Element element) {
        return CassandraArchiveOperationHandler.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);

        String gigaSpace = element.getAttribute(GIGA_SPACE);
        if (StringUtils.hasLength(gigaSpace)) {
            builder.addPropertyReference("gigaSpace", gigaSpace);
        }

        String keyspace = element.getAttribute(CASSANDRA_KEYSPACE);
        if (StringUtils.hasLength(keyspace)) {
            builder.addPropertyValue("keyspace", keyspace);
        }

        String hosts = element.getAttribute(CASSANDRA_HOSTS);
        if (StringUtils.hasLength(hosts)) {
            builder.addPropertyValue("hosts", hosts);
        }

        String port = element.getAttribute(CASSANDRA_PORT);
        if (StringUtils.hasLength(port)) {
            builder.addPropertyValue("port", port);
        }

        String consistency = element.getAttribute(CASSANDRA_CONSISTENCY);
        if (StringUtils.hasLength(consistency)) {
            builder.addPropertyValue("writeConsistency", consistency);
        }
    }

}
