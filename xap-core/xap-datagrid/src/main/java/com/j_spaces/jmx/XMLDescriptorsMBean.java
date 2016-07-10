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

package com.j_spaces.jmx;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Hashtable;
import java.util.Map;
import java.util.StringTokenizer;

import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;


/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

public abstract class XMLDescriptorsMBean
        extends AbstractDynamicMBean {
    // secondary group
    private static final String ATTRIBUTE_TAG = "attribute";
    private static final String OPERATION_TAG = "operation";
    private static final String CONSTRUCTOR_TAG = "constructor";
    private static final String NOTIFIER_TAG = "notifier";
    // third group

    private static final String PARAMETER_TAG = "parameter";
    private static final String TYPES_TAG = "types";

    // common
    private static final String NAME_ATTR = "name";
    private static final String DESCRIPTION_ATTR = "description";
    // MBean
    private static final String TYPE_ATTR = "type";
    // attribute
    private static final String IS_READABLE_ATTR = "isReadable";
    private static final String IS_WRITABLE_ATTR = "isWritable";
    private static final String IS_IS_ATTR = "isIs";
    // operation
    private static final String IMPACT_ARRT = "impact";

    private String m_type;

    // xml schema validation file
    final static String VALID_SCHEMA_FILE = "/MBeanXMLDescriptors.xsd";

    // Constants used for JAXP 1.2
    static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
    static final String W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema";
    static final String JAXP_SCHEMA_LOCATION = "http://apache.org/xml/properties/schema/external-noNamespaceSchemaLocation";

    protected XMLDescriptorsMBean(String xmlFileURL)
            throws Exception {
        super();
        init(xmlFileURL);
    }

    protected XMLDescriptorsMBean() {
    }

    private void init(String xmlFileURL)
            throws Exception {
        // Obtaining a org.w3c.dom.Document from XML
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        /*factory.setValidating(true);
        factory.setAttribute(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
        factory.setAttribute(JAXP_SCHEMA_LOCATION, getClass().getResource( VALID_SCHEMA_FILE ).toString() );
        */
        DocumentBuilder builder = factory.newDocumentBuilder();

        Document rootDoc = builder.parse(xmlFileURL);
        Element rootElem = rootDoc.getDocumentElement();

        m_type = rootElem.getAttribute(TYPE_ATTR);
        setMBeanDescription(rootElem.getAttribute(DESCRIPTION_ATTR));

        // ************************
        // Process MBean attributes
        // ************************
        NodeList elementsList = rootElem.getElementsByTagName(ATTRIBUTE_TAG);
        for (int i = 0; i < elementsList.getLength(); i++) {
            Element element = (Element) elementsList.item(i);
            Map attrMap = getXmlAttributesFromNode(element);
            //System.out.println(element + ", " + attrMap);

            String descrPrefix = attrMap.get(NAME_ATTR) + ": ";
            addMBeanAttributeInfo(new MBeanAttributeInfo(
                    (String) attrMap.get(NAME_ATTR),
                    (String) attrMap.get(TYPE_ATTR),
                    descrPrefix + attrMap.get(DESCRIPTION_ATTR),
                    Boolean.valueOf((String) attrMap.get(IS_READABLE_ATTR)).booleanValue(),
                    Boolean.valueOf((String) attrMap.get(IS_WRITABLE_ATTR)).booleanValue(),
                    Boolean.valueOf((String) attrMap.get(IS_IS_ATTR)).booleanValue()));
        }

        // ************************
        // Process MBean operations
        // ************************
        elementsList = rootElem.getElementsByTagName(OPERATION_TAG);
        for (int i = 0; i < elementsList.getLength(); i++) {
            Element element = (Element) elementsList.item(i);
            Map attrMap = getXmlAttributesFromNode(element);
            //System.out.println(element + ", " + attrMap);
            addMBeanOperationInfo(new MBeanOperationInfo(
                    (String) attrMap.get(NAME_ATTR),
                    (String) attrMap.get(DESCRIPTION_ATTR),
                    getSignature(element.getElementsByTagName(PARAMETER_TAG)),
                    (String) attrMap.get(TYPE_ATTR),
                    convertImpact((String) attrMap.get(IMPACT_ARRT))));
        }

        // **************************
        // Process MBean constructors
        // **************************
        elementsList = rootElem.getElementsByTagName(CONSTRUCTOR_TAG);
        for (int i = 0; i < elementsList.getLength(); i++) {
            Element element = (Element) elementsList.item(i);
            Map attrMap = getXmlAttributesFromNode(element);
            //System.out.println(element + ", " + attrMap);
            addMBeanConstructorInfo(new MBeanConstructorInfo(
                    (String) attrMap.get(NAME_ATTR),
                    (String) attrMap.get(DESCRIPTION_ATTR),
                    getSignature(element.getElementsByTagName(PARAMETER_TAG))));
        }

        // ***********************
        // Process MBean notifiers
        // ***********************
        elementsList = rootElem.getElementsByTagName(NOTIFIER_TAG);
        for (int i = 0; i < elementsList.getLength(); i++) {
            Element element = (Element) elementsList.item(i);
            Map attrMap = getXmlAttributesFromNode(element);
            //System.out.println(element + ", " + attrMap);

            String types = getNodeValueIfExists(element, TYPES_TAG);
            String[] notifTypes = null;
            if (types != null && types.trim().length() > 0) {
                StringTokenizer st = new StringTokenizer(types.trim());
                notifTypes = new String[st.countTokens()];
                for (int j = 0; j < st.countTokens(); j++)
                    notifTypes[j] = st.nextToken();
            }

            addMBeanNotificationInfo(new MBeanNotificationInfo(
                    notifTypes,
                    (String) attrMap.get(NAME_ATTR),
                    (String) attrMap.get(DESCRIPTION_ATTR)));
        }
    }// init(String xmlFileName)

    private MBeanParameterInfo[] getSignature(NodeList paramList) {
        MBeanParameterInfo[] signature = null;
        if (paramList != null && paramList.getLength() > 0) {
            signature = new MBeanParameterInfo[paramList.getLength()];
            for (int j = 0; j < paramList.getLength(); j++) {
                Element paramElem = (Element) paramList.item(j);
                //System.out.println(paramElem + ", " + getXmlAttributesFromNode(paramElem));
                Map paramAttr = getXmlAttributesFromNode(paramElem);
                signature[j] = new MBeanParameterInfo(
                        (String) paramAttr.get(NAME_ATTR),
                        (String) paramAttr.get(TYPE_ATTR),
                        (String) paramAttr.get(DESCRIPTION_ATTR));
            }
        }
        return signature;
    }

    private Map getXmlAttributesFromNode(Element element) {
        Map result = new Hashtable();
        NamedNodeMap attrMap = element.getAttributes();
        for (int j = 0; j < attrMap.getLength(); j++) {
            Node node = attrMap.item(j);
            String nodeName = node.getNodeName();
            String nodeValue = node.getNodeValue();
            result.put(nodeName, nodeValue);
        }
        return result;
    }

    private String getNodeValueIfExists(Element parentNode, String nodeName) {
        String value = null;
        NodeList nl = parentNode.getElementsByTagName(nodeName);
        if (nl.getLength() > 0) {
            Node child = nl.item(0).getFirstChild();
            if (child != null)
                value = child.getNodeValue().trim();
        }
        return value;
    }

    private int convertImpact(String value)
            throws InvalidAttributeValueException {
        if (value.equals("ACTION"))
            return MBeanOperationInfo.ACTION;
        else if (value.equals("ACTION_INFO"))
            return MBeanOperationInfo.ACTION_INFO;
        else if (value.equals("INFO"))
            return MBeanOperationInfo.INFO;
        else if (value.equals("UNKNOWN"))
            return MBeanOperationInfo.UNKNOWN;
        else
            throw new InvalidAttributeValueException(
                    "The value of <" + OPERATION_TAG + "> " + IMPACT_ARRT +
                            " attribute, should be ACTION, ACTION_INFO, INFO or UNKNOWN");
    }


    public String getType() {
        if (m_type == null || m_type.trim().length() <= 0)
            m_type = this.getClass().getName();

        return m_type;
    }
}
