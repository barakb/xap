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

package com.gigaspaces.internal.xml;

import com.gigaspaces.internal.io.XmlUtils;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class XmlParser {

    private final Document xmlDocument;
    private final XPath xPath;

    private XmlParser(Document xmlDocument) {
        this.xmlDocument = xmlDocument;
        this.xPath = XPathFactory.newInstance().newXPath();
    }

    public static XmlParser fromPath(String fileName) {
        try {
            return new XmlParser(XmlUtils.getDocumentBuilder().parse(fileName));
        } catch (ParserConfigurationException e) {
            throw new RuntimeException("Failed to parse '" + fileName + "'", e);
        } catch (SAXException e) {
            throw new RuntimeException("Failed to parse '" + fileName + "'", e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse '" + fileName + "'", e);
        }
    }

    public NodeList getNodes(String expression) {
        try {
            return (NodeList) xPath.evaluate(expression, xmlDocument.getDocumentElement(), XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            throw new RuntimeException("Failed to evaluate XPath expression '" + expression + "'", e);
        }
    }

    public Node getNode(String expression) {
        try {
            return (Node) xPath.evaluate(expression, xmlDocument.getDocumentElement(), XPathConstants.NODE);
        } catch (XPathExpressionException e) {
            throw new RuntimeException("Failed to evaluate XPath expression '" + expression + "'", e);
        }
    }

    public static Properties parseProperties(Element element, String keyAttribute, String valueAttribute) {
        Properties properties = new Properties();
        NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node instanceof Element) {
                Element property = (Element) node;
                properties.put(property.getAttribute(keyAttribute), property.getAttribute(valueAttribute));
            }
        }

        return properties;
    }

}
