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

package org.openspaces.core.config.xmlparser;

import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author itaif
 * @since 9.0.1
 */
public class XmlBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    private final Class<?> clazz;
    private final XmlAnnotationExtractor annotations;


    @Override
    protected Class<?> getBeanClass(Element element) {
        return clazz;
    }

    public XmlBeanDefinitionParser(Class<?> clazz) {
        this.clazz = clazz;
        annotations = new XmlAnnotationExtractor(clazz);
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        NamedNodeMap attributes = element.getAttributes();
        
        /* parse attributes */
        for (int i = 0; i < attributes.getLength(); i++) {

            Attr attribute = (Attr) attributes.item(i);
            String attributeName = attribute.getName();
            Method method = annotations.getMethodPerAttributeName().get(attributeName);
            if (method != null) {
                String propertyName = convertSetterMethodToPropertyName(method);
                String value = attribute.getValue();
                builder.addPropertyValue(propertyName, value);
            }
        }

        /* parse child elements */
        Map<Method, ManagedList<Object>> listValues = new HashMap<Method, ManagedList<Object>>();
        for (Element child : DomUtils.getChildElements(element)) {

            String childName = child.getLocalName();

            Object value = parserContext.getDelegate().parsePropertySubElement(child, builder.getRawBeanDefinition());
            Method method = annotations.getMethodPerChildElementName().get(childName);
            if (method == null) {
                Class<?> valueType = null;
                if (value instanceof BeanDefinitionHolder) {
                    String beanClassName = ((BeanDefinitionHolder) value).getBeanDefinition().getBeanClassName();
                    try {
                        valueType = Class.forName(beanClassName);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalStateException("Cannot find class " + beanClassName + " inferred from xml "
                                + childName, e);
                    }
                } else if (value instanceof List) {
                    valueType = List.class;
                } else {
                    //TODO: Support references and other types
                }
                if (valueType != null) {
                    // look for method by element type
                    for (Entry<Class<?>, Method> pair :
                            annotations.getMethodPerChildElementType().entrySet()) {

                        if (pair.getKey().isAssignableFrom(valueType)) {
                            method = pair.getValue();
                            break;
                        }
                    }
                }
            }
            if (method != null) {
                if (!isListSetterMethod(method)) {
                    String propertyName = convertSetterMethodToPropertyName(method);
                    builder.addPropertyValue(propertyName, value);
                } else {
                    // setter of list is a special case since each child element adds
                    // only one instance.
                    ManagedList<Object> values = listValues.get(method);
                    if (values == null) {
                        values = new ManagedList<Object>();
                    }
                    values.add(value);

                    listValues.put(method, values);
                }
            }
        }

        // Set list values
        for (Entry<Method, ManagedList<Object>> pair : listValues.entrySet()) {
            String propertyName = convertSetterMethodToPropertyName(pair.getKey());
            builder.addPropertyValue(propertyName, pair.getValue());
        }

    }


    /**
     * Auto generating bean ids to avoid the error Configuration problem: Id is required for element
     * when used as a top-level tag
     */
    @Override
    protected boolean shouldGenerateIdAsFallback() {
        return true;
    }

    private static String convertSetterMethodToPropertyName(Method method) {
        String methodName = method.getName();
        if (!methodName.startsWith("set")) {
            throw new IllegalArgumentException(methodName + " does not start with 'set':" + methodName);
        }
        return String.valueOf(methodName.charAt(3)).toLowerCase() + methodName.substring(4);
    }

    private static boolean isListSetterMethod(Method method) {
        Class<?> type = method.getParameterTypes()[0];
        return type.equals(List.class) || type.isArray();
    }
}
