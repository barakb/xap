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


package org.openspaces.jdbc.config;

import org.openspaces.jdbc.datasource.SpaceDriverManagerDataSource;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.w3c.dom.Element;

/**
 * A bean definition builder for {@link SpaceDriverManagerDataSource}.
 *
 * @author kimchy
 */
public class DataSourceBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    protected Class<SpaceDriverManagerDataSource> getBeanClass(Element element) {
        return SpaceDriverManagerDataSource.class;
    }

    protected void doParse(Element element, BeanDefinitionBuilder builder) {
        String space = element.getAttribute("space");
        builder.addPropertyReference("space", space);
    }
}
