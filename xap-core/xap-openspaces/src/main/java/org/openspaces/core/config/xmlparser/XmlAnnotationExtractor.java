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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElement.DEFAULT;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlTransient;

/**
 * @author itaif
 * @since 9.0.1
 */
public class XmlAnnotationExtractor {

    private final Class<?> clazz;
    private final Map<String, Method> methodPerAttributeName = new HashMap<String, Method>();
    private final Map<Class<?>, Method> methodPerChildElementType = new HashMap<Class<?>, Method>();
    private final Map<String, Method> methodPerChildElementName = new HashMap<String, Method>();

    public Map<String, Method> getMethodPerAttributeName() {
        return methodPerAttributeName;
    }

    public Map<String, Method> getMethodPerChildElementName() {
        return methodPerChildElementName;
    }

    public Map<Class<?>, Method> getMethodPerChildElementType() {
        return this.methodPerChildElementType;
    }

    public XmlAnnotationExtractor(Class<?> clazz) {
        this.clazz = clazz;

        for (final Method method : clazz.getMethods()) {

            XmlTransient xmlTransient = method.getAnnotation(XmlTransient.class);
            if (xmlTransient != null) {
                //transient annotation means to ignore the method
                continue;
            }

            XmlAttribute xmlAttributeAnnotation = method.getAnnotation(XmlAttribute.class);
            if (xmlAttributeAnnotation != null) {
                validateSetterMethod(method);
                String name = xmlAttributeAnnotation.name();
                if (name.equals("##default")) {
                    name = getXmlNameFromMethodName(method);
                }
                putMethodByAttributeName(name, method);
            }

            XmlElement xmlElementAnnotation = method.getAnnotation(XmlElement.class);
            if (xmlElementAnnotation != null) {
                handleXmlElementAnnotation(method, xmlElementAnnotation);
            }

            XmlElements xmlElementsAnnotation = method.getAnnotation(XmlElements.class);
            if (xmlElementsAnnotation != null) {
                for (XmlElement innerXmlElementAnnotation : xmlElementsAnnotation.value()) {
                    if (innerXmlElementAnnotation != null) {
                        handleXmlElementAnnotation(method, innerXmlElementAnnotation);
                    }
                }
            }

            if (xmlAttributeAnnotation == null &&
                    xmlElementAnnotation == null &&
                    xmlElementsAnnotation == null &&
                    isSetterMethod(method)) {

                final String name = getXmlNameFromMethodName(method);
                if (isAssignableFromString(method)) {
                    putMethodByAttributeName(name, method);
                } else {
                    putMethodByElementName(name, method);
                }
            }
        }
    }

    private void handleXmlElementAnnotation(final Method method, XmlElement xmlElementAnnotation) {
        validateSetterMethod(method);

        final boolean defaultName = xmlElementAnnotation.name().equals("##default");
        final boolean defaultType = xmlElementAnnotation.type().equals(DEFAULT.class);
        if (!defaultName) {
            putMethodByElementName(xmlElementAnnotation.name(), method);
        } else if (!defaultType) {
            putMethodByElementType(xmlElementAnnotation.type(), method);
        } else {
            putMethodByElementName(getXmlNameFromMethodName(method), method);
        }
    }

    private boolean isAssignableFromString(Method method) {

        Class<?> type = method.getParameterTypes()[0];
        if (type.isPrimitive()) {
            return true;
        }
        for (Constructor<?> cotr : type.getConstructors()) {
            if (cotr.getParameterTypes().length == 1 && cotr.getParameterTypes()[0].equals(String.class)) {
                return true;
            }
        }

        if (type.isArray() && String.class.isAssignableFrom(type.getComponentType())) {
            return true;
        }
        return false;
    }

    private String getXmlNameFromMethodName(final Method method) {
        final String propertyName = convertSetterMethodToPropertyName(method);
        final String attributeName = convertPropertyNameToAttributeName(propertyName);
        return attributeName;
    }

    private void putMethodByElementName(final String elementName, final Method method) {
        if (methodPerChildElementName.containsKey(elementName)) {
            throw new IllegalStateException(XmlElement.class.getName() + " conflict in class "
                    + clazz.getName() + ". " + elementName + " defined both in method "
                    + method.getName() + " and "
                    + methodPerAttributeName.get(elementName).getName());
        }
        methodPerChildElementName.put(elementName, method);
    }

    private void putMethodByAttributeName(final String attributeName, final Method method) {
        if (methodPerAttributeName.containsKey(attributeName)) {
            throw new IllegalStateException(XmlAttribute.class.getName() + " conflict in class "
                    + clazz.getName() + ". " + attributeName + " defined both in method "
                    + method.getName() + " and "
                    + methodPerAttributeName.get(attributeName).getName());
        }
        methodPerAttributeName.put(attributeName, method);
    }

    private Method putMethodByElementType(Class<?> type, final Method method) {
        if (methodPerChildElementType.containsKey(type)) {
            throw new IllegalStateException(XmlElement.class.getName() + " conflict in class "
                    + clazz.getName() + ". Type " + type.getName() + " defined both in method "
                    + method.getName() + " and "
                    + methodPerChildElementType.get(type).getName());
        }
        return methodPerChildElementType.put(type, method);
    }

    private String convertSetterMethodToPropertyName(Method method) {

        String methodName = method.getName();
        String propertyName = String.valueOf(methodName.charAt(3)).toLowerCase() + methodName.substring(4);
        return propertyName;
    }

    private static boolean isSetterMethod(Method method) {
        return method.getName().startsWith("set") && method.getParameterTypes().length == 1;
    }

    private static void validateSetterMethod(Method method) {
        if (!method.getName().startsWith("set")) {
            throw new IllegalArgumentException(method.getName() + " must start with 'set' prefix or use a value for the @XmlProperty annotation");
        }
        if (method.getParameterTypes().length != 1) {
            throw new IllegalStateException("Method " + method.getName()
                    + " must have a single parameter since it is marked with the "
                    + XmlAttribute.class.getName() + " attribute");
        }
    }

    private static String convertPropertyNameToAttributeName(String propertyName) {
        StringBuilder attributeName = new StringBuilder();
        if (propertyName.isEmpty()) {
            throw new IllegalArgumentException("propertyName cannot be empty");
        }
        if (Character.isUpperCase(propertyName.charAt(0))) {
            throw new IllegalArgumentException("propertyName " + propertyName + " must start with a lowercase letter");
        }
        for (int i = 0; i < propertyName.length(); i++) {
            char c = propertyName.charAt(i);
            if (Character.isUpperCase(c)) {
                attributeName.append('-').append(Character.toLowerCase(c));
            } else {
                attributeName.append(c);
            }
        }
        return attributeName.toString();
    }

}
