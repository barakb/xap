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

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.Properties;

@com.gigaspaces.api.InternalApi
public class XmlParserTest {

    @Test
    public void testXPath() {
        XmlParser parser = XmlParser.fromPath(this.getClass().getResource("test.xml").toString());

        testNodes(parser, "/foo/bars/bar0", new String[]{});
        testNodes(parser, "/foo/bars/bar1", new String[]{"one"});
        testNodes(parser, "/foo/bars/bar2", new String[]{"two", "three"});
    }

    @Test
    public void testProperties() {
        XmlParser parser = XmlParser.fromPath(this.getClass().getResource("test.xml").toString());

        Properties properties = XmlParser.parseProperties((Element) parser.getNode("/foo/bars/bar1"), "key", "value");
        Assert.assertEquals(2, properties.size());
        Assert.assertEquals("one", properties.getProperty("1"));
        Assert.assertEquals("two", properties.getProperty("2"));
    }

    private void testNodes(XmlParser parser, String expression, String[] names) {
        NodeList nodes = parser.getNodes(expression);
        Assert.assertEquals(names.length, nodes.getLength());
        for (int i = 0; i < names.length; i++)
            Assert.assertEquals(names[i], nodes.item(i).getAttributes().item(0).getNodeValue());
    }
}
