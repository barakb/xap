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

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

import javax.xml.bind.annotation.XmlRootElement;

public abstract class AbstractXmlBeanNamespaceHandler extends NamespaceHandlerSupport {

    protected void register(Class<?> clazz) {
        String elementName;
        XmlRootElement rootElement = clazz.getAnnotation(XmlRootElement.class);
        if (rootElement == null || rootElement.name().equals("##default")) {
            elementName = convertClassNameToAttributeName(clazz.getSimpleName());
        } else {
            elementName = rootElement.name();
            if (elementName == null || elementName.length() == 0) {
                throw new IllegalArgumentException("Class " + clazz.getName() + " " + XmlRootElement.class.getName() + " annotation name cannot be null or empty");
            }
        }
        registerBeanDefinitionParser(elementName, new XmlBeanDefinitionParser(clazz));
    }

    private static String convertClassNameToAttributeName(String classname) {
        StringBuilder attributeName = new StringBuilder();
        if (classname.isEmpty()) {
            throw new IllegalArgumentException("classname cannot be empty");
        }
        if (!Character.isUpperCase(classname.charAt(0))) {
            throw new IllegalArgumentException("classname " + classname + " must start with an uppercase letter");
        }

        for (int i = 0; i < classname.length(); i++) {
            char c = classname.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                attributeName.append('-').append(Character.toLowerCase(c));
            } else {
                attributeName.append(Character.toLowerCase(c));
            }
        }
        return attributeName.toString();
    }

}
