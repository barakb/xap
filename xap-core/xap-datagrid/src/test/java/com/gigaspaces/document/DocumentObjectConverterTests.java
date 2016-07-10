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

import com.gigaspaces.document.pojos.Address;
import com.gigaspaces.document.pojos.ConstructorBasedPojo;
import com.gigaspaces.document.pojos.ForeignAddress;
import com.gigaspaces.document.pojos.Person;
import com.gigaspaces.document.pojos.PojoArrays;
import com.gigaspaces.document.pojos.PojoCollections;
import com.gigaspaces.document.pojos.PojoCommonTypes;
import com.gigaspaces.document.pojos.PojoDynamicProperties;
import com.gigaspaces.document.pojos.PojoNestedCyclic;
import com.gigaspaces.document.pojos.PojoNestedCyclic.Leaf;
import com.gigaspaces.document.pojos.PojoNestedCyclic.Node;
import com.gigaspaces.document.pojos.PojoNestedObject;
import com.gigaspaces.document.pojos.PojoNoProperties;
import com.gigaspaces.document.pojos.PojoNullValues;
import com.gigaspaces.document.pojos.PojoOneProperty;
import com.gigaspaces.document.pojos.PojoWithComplexJavaProperties;
import com.gigaspaces.metadata.SpaceDocumentSupport;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class DocumentObjectConverterTests {
    private final DocumentObjectConverter _converter = new DocumentObjectConverter();

    @Test
    public void testNull() {
        SpaceDocument document = _converter.toSpaceDocument(null);
        Assert.assertNull(document);

        Object object = _converter.toObject(null);
        Assert.assertNull(object);
    }

    @Test
    public void testObject() {
        Object object = new Object();

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 0);

        Object object2 = _converter.toObject(document);
        Assert.assertNotNull("object2", object2);
        Assert.assertEquals("class", object.getClass(), object2.getClass());
    }

    @Test
    public void testSpaceDocument() {
        Object object = new SpaceDocument();

        SpaceDocument document = _converter.toSpaceDocument(object);
        Assert.assertSame(object, document);
    }

    @Test
    public void testPojoNoProperties() {
        PojoNoProperties object = new PojoNoProperties();

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 0);

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    @Test
    public void testPojoOneProperty() {
        PojoOneProperty object = new PojoOneProperty();
        object.setFoo("bar");

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        assertDocumentPropertySame(document, "foo", "bar");

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    @Test
    public void testPojoCommonTypes() {
        PojoCommonTypes object = new PojoCommonTypes();
        object.setPropByte((byte) 1);
        object.setPropShort((short) 10);
        object.setPropInt(100);
        object.setPropLong(1000);
        object.setPropFloat(0.1F);
        object.setPropDouble(0.01);
        object.setPropBoolean(true);
        object.setPropChar('g');
        object.setPropByteWrapper((byte) 2);
        object.setPropShortWrapper((short) 20);
        object.setPropIntWrapper(200);
        object.setPropLongWrapper(new Long(2000));
        object.setPropFloatWrapper(0.2F);
        object.setPropDoubleWrapper(0.02);
        object.setPropBooleanWrapper(true);
        object.setPropCharWrapper('G');

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 16);
        assertDocumentPropertyEquals(document, "propByte", object.getPropByte());
        assertDocumentPropertyEquals(document, "propShort", object.getPropShort());
        assertDocumentPropertyEquals(document, "propInt", object.getPropInt());
        assertDocumentPropertyEquals(document, "propLong", object.getPropLong());
        assertDocumentPropertyEquals(document, "propFloat", object.getPropFloat());
        assertDocumentPropertyEquals(document, "propDouble", object.getPropDouble());
        assertDocumentPropertyEquals(document, "propBoolean", object.getPropBoolean());
        assertDocumentPropertyEquals(document, "propChar", object.getPropChar());
        assertDocumentPropertySame(document, "propByteWrapper", object.getPropByteWrapper());
        assertDocumentPropertySame(document, "propShortWrapper", object.getPropShortWrapper());
        assertDocumentPropertySame(document, "propIntWrapper", object.getPropIntWrapper());
        assertDocumentPropertySame(document, "propLongWrapper", object.getPropLongWrapper());
        assertDocumentPropertySame(document, "propFloatWrapper", object.getPropFloatWrapper());
        assertDocumentPropertySame(document, "propDoubleWrapper", object.getPropDoubleWrapper());
        assertDocumentPropertySame(document, "propBooleanWrapper", object.getPropBooleanWrapper());
        assertDocumentPropertySame(document, "propCharWrapper", object.getPropCharWrapper());

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    @Test
    public void testPojoDynamicProperties() {
        PojoDynamicProperties object = new PojoDynamicProperties();
        object.setFoo("bar");
        object.setDynamicProperties(new HashMap<String, Object>());
        object.getDynamicProperties().put("bar", 666);

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 2);
        assertDocumentPropertySame(document, "foo", "bar");
        assertDocumentPropertyEquals(document, "bar", 666);

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    @Test
    public void testPojoNullValues() {
        PojoNullValues object = new PojoNullValues();
        object.setFoo(1);

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        assertDocumentPropertySame(document, "foo", 1);

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);

        object.setFoo(-1);

        document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        Assert.assertTrue(document.containsProperty("foo"));
        assertDocumentPropertySame(document, "foo", null);

        object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
        Assert.assertEquals(-1, ((PojoNullValues) object2).getFoo());
    }

    @Test
    public void testPojoNestedPojos() {
        Person object = new Person()
                .setName("Kermit")
                .setAge(7)
                .setHomeAddress(new Address().setStreet("sesame").setHouseNumber(123))
                .setWorkAddress(new Address().setStreet("Main").setHouseNumber(1))
                .setMailAddress(new Address().setStreet("Elm").setHouseNumber(666));

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 5);
        assertDocumentPropertySame(document, "name", "Kermit");
        assertDocumentPropertySame(document, "age", 7);
        assertDocumentPropertySame(document, "workAddress", object.getWorkAddress());
        SpaceDocument homeAddressDocument = document.getProperty("homeAddress");
        assertDocumentPropertySame(homeAddressDocument, "street", object.getHomeAddress().getStreet());
        assertDocumentPropertySame(homeAddressDocument, "houseNumber", object.getHomeAddress().getHouseNumber());
        SpaceDocument mailAddressDocument = document.getProperty("mailAddress");
        assertDocumentPropertySame(mailAddressDocument, "street", object.getMailAddress().getStreet());
        assertDocumentPropertyEquals(mailAddressDocument, "houseNumber", object.getMailAddress().getHouseNumber());

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    @Test
    public void testPojoNestedPojosNulls() {
        Person object = new Person();

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 5);
        assertDocumentPropertySame(document, "name", null);
        assertDocumentPropertySame(document, "age", 0);
        Assert.assertNull(document.getProperty("homeAddress"));
        Assert.assertNull(document.getProperty("workAddress"));
        Assert.assertNull(document.getProperty("mailAddress"));

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    @Test
    public void testPojoNestedPojosPolymorphism() {
        Person object = new Person()
                .setName("Kermit")
                .setAge(7)
                .setHomeAddress(new ForeignAddress().setCountry("Israel").setStreet("sesame").setHouseNumber(123))
                .setWorkAddress(new ForeignAddress().setCountry("U.S.A").setStreet("Main").setHouseNumber(1))
                .setMailAddress(new ForeignAddress().setCountry("LaLaLand").setStreet("Elm").setHouseNumber(666));

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 5);
        assertDocumentPropertySame(document, "name", "Kermit");
        assertDocumentPropertySame(document, "age", 7);
        assertDocumentPropertySame(document, "workAddress", object.getWorkAddress());

        SpaceDocument expectedHomeAddress = new SpaceDocument(ForeignAddress.class.getName())
                .setProperty("street", object.getHomeAddress().getStreet())
                .setProperty("houseNumber", object.getHomeAddress().getHouseNumber())
                .setProperty("country", ((ForeignAddress) object.getHomeAddress()).getCountry());
        assertDocumentPropertyEquals(document, "homeAddress", expectedHomeAddress);

        SpaceDocument expectedMailAddress = new SpaceDocument(ForeignAddress.class.getName())
                .setProperty("street", object.getMailAddress().getStreet())
                .setProperty("houseNumber", object.getMailAddress().getHouseNumber())
                .setProperty("country", ((ForeignAddress) object.getMailAddress()).getCountry());
        assertDocumentPropertyEquals(document, "mailAddress", expectedMailAddress);

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    @Test
    public void testPojoNestedObject() {
        // Test null:
        PojoNestedObject object = new PojoNestedObject();
        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        assertDocumentPropertySame(document, "foo", null);
        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);

        // Test String:
        object.setFoo("bar");
        document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        assertDocumentPropertySame(document, "foo", "bar");
        object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);

        // Test Integer:
        object.setFoo(1);
        document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        assertDocumentPropertySame(document, "foo", 1);
        object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);

        // Test User class:
        object.setFoo(new Address().setStreet("Main").setHouseNumber(7));
        document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        assertDocumentPropertyEquals(document, "foo", new SpaceDocument(Address.class.getName())
                .setProperty("street", "Main")
                .setProperty("houseNumber", 7));
        object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);

        // Test Object:
        object.setFoo(new Object());
        document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        assertDocumentPropertySame(document, "foo", object.getFoo());
        object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);

        // Test Map:
        object.setFoo(new DocumentProperties().setProperty("bar", "ref"));
        document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        DocumentProperties foo = (DocumentProperties) object.getFoo();
        DocumentProperties expectedFoo = (DocumentProperties) _converter.toDocumentIfNeeded(foo, SpaceDocumentSupport.DEFAULT);
        assertDocumentPropertyEquals(document, "foo", expectedFoo);
        object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);

        // Test Map with complex types
        Address homeAddress = new Address().setHouseNumber(3).setStreet("street");
        Person person = new Person().setAge(3).setName("person").setHomeAddress(homeAddress);
        object.setFoo(new DocumentProperties().setProperty("person", person));
        document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 1);
        DocumentProperties fooPerson = (DocumentProperties) object.getFoo();
        DocumentProperties expectedFooPerson = (DocumentProperties) _converter.toDocumentIfNeeded(fooPerson, SpaceDocumentSupport.DEFAULT);
        assertDocumentPropertyEquals(document, "foo", expectedFooPerson);
        object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    @Test
    public void testPojoCollections() {
        PojoCollections object = new PojoCollections();
        object.setByteList(createList((byte) 1, (byte) 2, null));
        object.setShortList(createList((short) 10, (short) 20, null));
        object.setIntList(createList(100, 200, null));
        object.setLongList(createList((long) 1000, (long) 2000, null));
        object.setFloatList(createList((float) 0.1, (float) 0.2, null));
        object.setDoubleList(createList(0.01, 0.02, null));
        object.setBooleanList(createList(true, false, null));
        object.setCharList(createList('a', 'Z', null));
        object.setObjectList(createList(1, "string", null, new Object(), new PojoOneProperty().setFoo("bar")));
        object.setPersonList1(createList(new Person().setName("foo"), null, new Person().setAge(2)));
        object.setPersonList2(createList(new Person().setName("bar"), null, new Person().setAge(4)));

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 11);
        assertDocumentPropertyEquals(document, "byteList", object.getByteList());
        assertDocumentPropertyEquals(document, "shortList", object.getShortList());
        assertDocumentPropertyEquals(document, "intList", object.getIntList());
        assertDocumentPropertyEquals(document, "longList", object.getLongList());
        assertDocumentPropertyEquals(document, "floatList", object.getFloatList());
        assertDocumentPropertyEquals(document, "doubleList", object.getDoubleList());
        assertDocumentPropertyEquals(document, "booleanList", object.getBooleanList());
        assertDocumentPropertyEquals(document, "charList", object.getCharList());
        List<Object> expectedObjectList = createList(
                object.getObjectList().get(0),
                object.getObjectList().get(1),
                object.getObjectList().get(2),
                object.getObjectList().get(3),
                _converter.toSpaceDocument(object.getObjectList().get(4)));
        assertDocumentPropertyEquals(document, "objectList", expectedObjectList);
        List<SpaceDocument> expectedPersonList = createList(
                _converter.toSpaceDocument(object.getPersonList1().get(0)),
                null,
                _converter.toSpaceDocument(object.getPersonList1().get(2)));
        assertDocumentPropertyEquals(document, "personList1", expectedPersonList);
        assertDocumentPropertySame(document, "personList2", object.getPersonList2());

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    @Test
    public void testPojoArrays() {
        PojoArrays object = new PojoArrays();
        object.setByteArray(new byte[]{(byte) 1, (byte) 2});
        object.setShortArray(new short[]{(short) 10, (short) 20});
        object.setIntArray(new int[]{100, 200});
        object.setLongArray(new long[]{(long) 1000, (long) 2000});
        object.setFloatArray(new float[]{(float) 0.1, (float) 0.2});
        object.setDoubleArray(new double[]{0.01, 0.02});
        object.setBooleanArray(new boolean[]{true, false});
        object.setCharArray(new char[]{'a', 'Z'});
        object.setByteWrapperArray(new Byte[]{(byte) 1, (byte) 2, null});
        object.setShortWrapperArray(new Short[]{(short) 10, (short) 20, null});
        object.setIntWrapperArray(new Integer[]{100, 200, null});
        object.setLongWrapperArray(new Long[]{(long) 1000, (long) 2000, null});
        object.setFloatWrapperArray(new Float[]{(float) 0.1, (float) 0.2, null});
        object.setDoubleWrapperArray(new Double[]{0.01, 0.02, null});
        object.setBooleanWrapperArray(new Boolean[]{true, false, null});
        object.setCharWrapperArray(new Character[]{'a', 'Z', null});
        object.setObjectArray(new Object[]{1, "string", null, new Object(), new PojoOneProperty().setFoo("bar")});
        object.setPersonArray1(new Person[]{new Person().setName("foo"), null, new Person().setAge(2)});
        object.setPersonArray2(new Person[]{new Person().setName("bar"), null, new Person().setAge(4)});

        SpaceDocument document = _converter.toSpaceDocument(object);
        assertDocument(document, object.getClass(), 19);
        assertDocumentPropertySame(document, "byteArray", object.getByteArray());
        assertDocumentPropertySame(document, "shortArray", object.getShortArray());
        assertDocumentPropertySame(document, "intArray", object.getIntArray());
        assertDocumentPropertySame(document, "longArray", object.getLongArray());
        assertDocumentPropertySame(document, "floatArray", object.getFloatArray());
        assertDocumentPropertySame(document, "doubleArray", object.getDoubleArray());
        assertDocumentPropertySame(document, "booleanArray", object.getBooleanArray());
        assertDocumentPropertySame(document, "charArray", object.getCharArray());
        assertDocumentPropertySame(document, "byteWrapperArray", object.getByteWrapperArray());
        assertDocumentPropertySame(document, "shortWrapperArray", object.getShortWrapperArray());
        assertDocumentPropertySame(document, "intWrapperArray", object.getIntWrapperArray());
        assertDocumentPropertySame(document, "longWrapperArray", object.getLongWrapperArray());
        assertDocumentPropertySame(document, "floatWrapperArray", object.getFloatWrapperArray());
        assertDocumentPropertySame(document, "doubleWrapperArray", object.getDoubleWrapperArray());
        assertDocumentPropertySame(document, "booleanWrapperArray", object.getBooleanWrapperArray());
        assertDocumentPropertySame(document, "charWrapperArray", object.getCharWrapperArray());
        Object[] expectedObjectArray = new Object[]{
                object.getObjectArray()[0],
                object.getObjectArray()[1],
                object.getObjectArray()[2],
                object.getObjectArray()[3],
                _converter.toSpaceDocument(object.getObjectArray()[4])};
        assertDocumentPropertyEquals(document, "objectArray", expectedObjectArray);
        SpaceDocument[] expectedPersonList = new SpaceDocument[]{
                _converter.toSpaceDocument(object.getPersonArray1()[0]),
                null,
                _converter.toSpaceDocument(object.getPersonArray1()[2])};
        assertDocumentPropertyEquals(document, "personArray1", expectedPersonList);
        assertDocumentPropertySame(document, "personArray2", object.getPersonArray2());

        Object object2 = _converter.toObject(document);
        Assert.assertEquals("object2", object, object2);
    }

    private SpaceDocument createExpectedSpaceDocumentRoot(Node root) {
        Leaf leaf1 = (Leaf) root.getChildren().get(0);
        Node child1 = root.getChildren().get(1);
        Leaf leaf2 = (Leaf) child1.getChildren().get(0);

        String NodeClassName = Node.class.getName();
        String LeafClassName = Leaf.class.getName();

        SpaceDocument expectedRootDocument = new SpaceDocument(NodeClassName);
        SpaceDocument expectedChild1Document = new SpaceDocument(NodeClassName);
        SpaceDocument expectedLeaf1Document = new SpaceDocument(LeafClassName);
        SpaceDocument expectedLeaf2Document = new SpaceDocument(LeafClassName);

        Map<String, Object> expectedChild1Properties = new HashMap<String, Object>();
        expectedChild1Properties.put("id", "child1");
        expectedChild1Properties.put("father", expectedRootDocument);
        List<SpaceDocument> expectedChild1ChildrenList = new LinkedList<SpaceDocument>();
        expectedChild1ChildrenList.add(expectedLeaf2Document);
        expectedChild1Properties.put("children", expectedChild1ChildrenList);
        expectedChild1Document.addProperties(expectedChild1Properties);

        Map<String, Object> expectedLeaf1Properties = new HashMap<String, Object>();
        expectedLeaf1Properties.put("id", "leaf1");
        expectedLeaf1Properties.put("father", expectedRootDocument);
        expectedLeaf1Properties.put("children", null);
        expectedLeaf1Properties.put("rightSibling", expectedLeaf2Document);
        expectedLeaf1Properties.put("leftSibling", null);
        expectedLeaf1Properties.put("data", leaf1.getData());
        expectedLeaf1Document.addProperties(expectedLeaf1Properties);

        Map<String, Object> expectedLeaf2Properties = new HashMap<String, Object>();
        expectedLeaf2Properties.put("id", "leaf2");
        expectedLeaf2Properties.put("father", expectedChild1Document);
        expectedLeaf2Properties.put("children", null);
        expectedLeaf2Properties.put("rightSibling", null);
        expectedLeaf2Properties.put("leftSibling", expectedLeaf1Document);
        expectedLeaf2Properties.put("data", leaf2.getData());
        expectedLeaf2Document.addProperties(expectedLeaf2Properties);

        Map<String, Object> expectedRootProperties = new HashMap<String, Object>();
        expectedRootProperties.put("id", "root");
        expectedRootProperties.put("father", null);
        List<SpaceDocument> expectedRootChildrenList = new LinkedList<SpaceDocument>();
        expectedRootChildrenList.add(expectedChild1Document);
        expectedRootChildrenList.add(expectedLeaf1Document);
        expectedRootProperties.put("children", expectedRootChildrenList);
        expectedRootDocument.addProperties(expectedRootProperties);

        return expectedRootDocument;
    }

    @Test
    public void testPojoConstructorBasedProperties() {
        ConstructorBasedPojo pojo = new ConstructorBasedPojo(1, 2, "mate", Long.MAX_VALUE, 1, true);
        SpaceDocument document = _converter.toSpaceDocument(pojo);
        ConstructorBasedPojo convertedPojo = (ConstructorBasedPojo) _converter.toObject(document);
        Assert.assertEquals("Bad conversion", pojo, convertedPojo);
    }

    @Test
    public void testPojoWithComplexJavaProperties() {
        PojoWithComplexJavaProperties pojo = new PojoWithComplexJavaProperties();
        pojo.setEntityClass(String.class);
        pojo.setEntityLocale(Locale.ENGLISH);
        pojo.setEntityURI(URI.create("http://www.google.com"));

        SpaceDocument document = _converter.toSpaceDocument(pojo);
        PojoWithComplexJavaProperties convertedPojo = (PojoWithComplexJavaProperties) _converter.toObject(document);
        Assert.assertEquals("Bad conversion", pojo, convertedPojo);
    }

    @Test
    public void testPojoNestedCyclic() {
        PojoNestedCyclic pojo = PojoNestedCyclic.createMixedTree();
        SpaceDocument document = _converter.toSpaceDocument(pojo);

        assertDocument(document, pojo.getClass(), 3);

        assertDocumentPropertyEquals(document, "totalLeaves", pojo.getTotalLeaves());
        PojoNestedCyclic.validateCyclicTree(document);
//        assertCyclicDocumentPropertyEquals(document, "root", createExpectedSpaceDocumentRoot(pojo.getRoot()));
        assertDocumentPropertyEquals(document, "time", pojo.getTime());

//        Object object = _converter.toObject(document);
//        assertDocumentPropertySame(document, "root", pojo.getRoot());
//        assertDocumentPropertySame(document, "totalLeaves", pojo.getTotalLeaves());
//        assertDocumentPropertySame(document, "time", pojo.getTime());
//        ObjectUtils.cyclicReferenceSupportedEqual(pojo, object);
    }

//    private void assertCyclicDocumentPropertyEquals(SpaceDocument document, String propertyName, SpaceDocument expectedValue) 
//    {
//        if (!document.containsProperty(propertyName))
//            Assert.fail("Document does not contain property " + propertyName);
//        Object actualValue = document.getProperty(propertyName);
//        Assert.assertTrue(actualValue instanceof SpaceDocument);
//        Assert.assertTrue(ObjectUtils.cyclicReferenceSupportedEqual(actualValue, expectedValue));
//    }

    private void assertDocument(SpaceDocument document, Class<?> expectedType, int expectedNumOfProperties) {
        Assert.assertNotNull("document", document);
        Assert.assertEquals("typeName", expectedType.getName(), document.getTypeName());
        Assert.assertEquals("numOfProperties", expectedNumOfProperties, document.getProperties().size());
    }

    private void assertDocumentPropertySame(SpaceDocument document, String propertyName, Object expectedValue) {
        if (!document.containsProperty(propertyName))
            Assert.fail("Document does not contain property " + propertyName);
        Object actualValue = document.getProperty(propertyName);
        Assert.assertSame("document." + propertyName, expectedValue, actualValue);
    }

    private void assertDocumentPropertyEquals(SpaceDocument document, String propertyName, Object expectedValue) {
        if (!document.containsProperty(propertyName))
            Assert.fail("Document does not contain property " + propertyName);
        Object actualValue = document.getProperty(propertyName);
        if (expectedValue instanceof Object[])
            assertArrayEquals((Object[]) expectedValue, (Object[]) actualValue);
        else
            Assert.assertEquals("document." + propertyName, expectedValue, actualValue);
    }

    private static <T> List<T> createList(T... items) {
        List<T> list = new ArrayList<T>(items.length);
        for (T item : items)
            list.add(item);
        return list;
    }

    private static <T> void assertArrayEquals(T[] array1, T[] array2) {
        if (array1 == array2)
            return;

        Assert.assertNotNull(array1);
        Assert.assertNotNull(array2);
        Assert.assertEquals(array1.length, array2.length);
        for (int i = 0; i < array1.length; i++)
            Assert.assertEquals(array1[i], array2[i]);
    }
}
