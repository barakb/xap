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

package com.gigaspaces.document;


import org.junit.Assert;
import org.junit.Test;

@com.gigaspaces.api.InternalApi
public class DocumentPropertiesTests {
    @Test
    public void testToString() {
        DocumentProperties properties = new DocumentProperties();

        // Test empty:
        Assert.assertEquals("DocumentProperties {}", properties.toString());
        // Add single mapping and test:
        properties.put("foo", "bar");
        Assert.assertEquals("DocumentProperties {foo=bar}", properties.toString());
        // Add second mapping and test:
        properties.put("bar", "barbar");
        Assert.assertEquals("DocumentProperties {bar=barbar,foo=bar}", properties.toString());
        // Remove initial key and test:
        properties.remove("foo");
        Assert.assertEquals("DocumentProperties {bar=barbar}", properties.toString());
        // Update 2nd key and test:
        properties.put("bar", "foo");
        Assert.assertEquals("DocumentProperties {bar=foo}", properties.toString());
    }

    @Test
    public void testEquals() {
        final DocumentProperties properties1 = new DocumentProperties();
        Assert.assertTrue(properties1.equals(properties1));
        Assert.assertFalse(properties1.equals(null));
        Assert.assertFalse(properties1.equals(new Object()));
        Assert.assertFalse(properties1.equals("foo"));

        final DocumentProperties properties2 = new DocumentProperties();
        Assert.assertTrue(properties1.equals(properties2));

        properties1.put("foo", "bar");
        Assert.assertFalse(properties1.equals(properties2));
        properties2.put("foo", "barbar");
        Assert.assertFalse(properties1.equals(properties2));
        properties2.put("foo", "bar");
        Assert.assertTrue(properties1.equals(properties2));
    }

    @Test
    public void testHashcode() {
        final DocumentProperties properties = new DocumentProperties();
        Assert.assertEquals(0, properties.hashCode());
    }
}
