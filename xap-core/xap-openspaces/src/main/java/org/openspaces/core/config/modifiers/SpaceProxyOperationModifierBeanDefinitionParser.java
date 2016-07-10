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

package org.openspaces.core.config.modifiers;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

/**
 * A bean definition parser for xxxxx-modifier elements defined in a giga-space element.
 *
 * @author Dan Kilman
 * @since 9.5
 */
public class SpaceProxyOperationModifierBeanDefinitionParser extends AbstractSimpleBeanDefinitionParser {

    private static final String WRITE_MODIFIER_TAG = "write-modifier";
    private static final String READ_MODIFIER_TAG = "read-modifier";
    private static final String TAKE_MODIFIER_TAG = "take-modifier";
    private static final String COUNT_MODIFIER_TAG = "count-modifier";
    private static final String CLEAR_MODIFIER_TAG = "clear-modifier";
    private static final String CHANGE_MODIFIER_TAG = "change-modifier";

    private static final String MODIFIER_NAME_PROPERTY = "modifierName";
    private static final String VALUE_PROPERTY = "value";

    @Override
    protected Class<?> getBeanClass(Element element) {
        String localName = element.getLocalName();
        if (WRITE_MODIFIER_TAG.equals(localName)) {
            return WriteModifierFactoryBean.class;
        } else if (READ_MODIFIER_TAG.equals(localName)) {
            return ReadModifierFactoryBean.class;
        } else if (TAKE_MODIFIER_TAG.equals(localName)) {
            return TakeModifierFactoryBean.class;
        } else if (COUNT_MODIFIER_TAG.equals(localName)) {
            return CountModifierFactoryBean.class;
        } else if (CLEAR_MODIFIER_TAG.equals(localName)) {
            return ClearModifierFactoryBean.class;
        } else if (CHANGE_MODIFIER_TAG.equals(localName)) {
            return ChangeModifierFactoryBean.class;
        }

        throw new IllegalArgumentException("Unsupported tag: " + element.getLocalName());
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        NamedNodeMap attributes = element.getAttributes();
        for (int x = 0; x < attributes.getLength(); x++) {
            Attr attribute = (Attr) attributes.item(x);
            String name = attribute.getLocalName();
            if (VALUE_PROPERTY.equals(name)) {
                builder.addPropertyValue(MODIFIER_NAME_PROPERTY, attribute.getValue());
                continue;
            }

            builder.addPropertyValue(name, attribute.getValue());
        }
    }

}
