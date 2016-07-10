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

package com.gigaspaces.persistency.archive;

import org.openspaces.archive.config.ArchiveNamespaceHandler;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Parses &lt;os-archive:mongo-archive-handler&gt;
 *
 * @author Shadi Massalha
 * @see ArchiveNamespaceHandler
 */
@SuppressWarnings("UnusedDeclaration")
public class MongoArchiveOperationHandlerBeanDefinitionParser extends
        AbstractSingleBeanDefinitionParser {

    private static final String GIGA_SPACE = "gigaSpace";
    private static final String CONFIG = "config";
    private static final String DB = "db";

    private static final String GIGA_SPACE_REF = "giga-space";
    private static final String CONFIG_REF = "config-ref";
    private static final String MONGO_DB = DB;


    @Override
    protected Class<MongoArchiveOperationHandler> getBeanClass(Element element) {
        return MongoArchiveOperationHandler.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext,
                           BeanDefinitionBuilder builder) {
        super.doParse(element, parserContext, builder);

        String gigaSpace = element.getAttribute(GIGA_SPACE_REF);
        if (StringUtils.hasLength(gigaSpace)) {
            builder.addPropertyReference(GIGA_SPACE, gigaSpace);
        }

        String config_ref = element.getAttribute(CONFIG_REF);
        if (StringUtils.hasLength(config_ref)) {
            builder.addPropertyReference(CONFIG, config_ref);
        }

        String db = element.getAttribute(MONGO_DB);
        if (StringUtils.hasLength(db)) {
            builder.addPropertyValue(DB, db);
        }
    }

}
