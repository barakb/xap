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

/*
 * @(#)ExternalizableTestHelperTest.java   Jan 1, 2008
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package com.j_spaces.core.cluster;

import junit.framework.TestCase;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;


/**
 * @author Barak Bar Orion
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class ExternalizableTestHelperTest extends TestCase {
    private ExternalizableTestHelper helper = new ExternalizableTestHelper();

    /**
     * boolean field should always assigned true.
     */
    public void testBooleanField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * integer field should always assigned 1.
     */
    public void testIntegerField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * long field should always assigned 1.
     */
    public void testLongField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * byte field should always assigned 1.
     */
    public void testByteField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * short field should always assigned 1.
     */
    public void testShortField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * char field should always assigned 1.
     */
    public void testCharField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * float field should always assigned 1.0
     */
    public void testFloatField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * double field should always assigned 1.0
     */
    public void testDoubleField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * AnObject field should always assigned new instance
     */
    public void testAnObjectField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * List field should always assigned new list with one elment 1L
     */
    public void testUntypedListField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        Object value = getPropertyValue(full, "untypedListField");
        assertTrue(value instanceof List);
        assertFalse(((List) value).isEmpty());
        assertEquals(1L, ((List) value).get(0));
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * List field should always assigned new list with one elment 1L
     */
    public void testUntypedLinkedListField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        Object value = getPropertyValue(full, "untypedLinkedListField");
        assertTrue(value instanceof List);
        assertFalse(((List) value).isEmpty());
        assertEquals(1L, ((List) value).get(0));
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * List field should always assigned new list with one elment 1L
     */
    public void testListOfStringsField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        Object value = getPropertyValue(full, "listOfStringsField");
        assertTrue(value instanceof List);
        assertFalse(((List) value).isEmpty());
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    /**
     * Test typed Map
     */
    public void testTypesHashMapField() throws Exception {
        AnObject empty = new AnObject();
        AnObject full = helper.fill(new AnObject());
        Object value = getPropertyValue(full, "mapField");
        assertTrue(value instanceof HashMap);
        assertTrue(helper.areEquals(full, full));
        assertFalse(helper.areEquals(empty, full));
    }

    private Object getPropertyValue(Object object, String name) {
        try {
            Field f = object.getClass().getDeclaredField(name);
            return f.get(object);
        } catch (NoSuchFieldException e) {
            System.out.println(e);
        } catch (IllegalAccessException e) {
            System.out.println(e);
        }
        return null;
    }
}

class AnObject {

    public AnObject() {
    }

    AnObject1 objectField;
    boolean boolField;
    Boolean booleanField;
    int intField;
    Integer integerField;
    long longField;
    Long long_field;
    byte byteField;
    Byte byte_field;
    short shortField;
    Short short_field;
    char charField;
    Character char_field;
    float floatField;
    Float float_field;
    double doubleField;
    Double double_field;
    List<String> listOfStringsField;
    LinkedList untypedLinkedListField;
    List untypedListField;
    HashMap<String, Properties> mapField;
}

class AnObject1 {

    public AnObject1() {
    }

    boolean boolField;
    Boolean booleanField;
    int intField;
    Integer integerField;
    long longField;
    Long long_field;
    byte byteField;
    Byte byte_field;
    short shortField;
    Short short_field;
    char charField;
    Character char_field;
    float floatField;
    Float float_field;
    double doubleField;
    Double double_field;
    List<String> listOfStringsField;
    LinkedList untypedLinkedListField;
    List untypedListField;
    HashMap<String, Properties> mapField;
}

