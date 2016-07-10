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

package com.gigaspaces.persistency.qa.model;

import com.gigaspaces.document.SpaceDocument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@SuppressWarnings("all")
public class TestDataTypeWithDynamicPropsUtils {

    private static final Random random = new Random();

    public static void assertTestDataEquals(
            TestDataTypeWithDynamicProps expected,
            TestDataTypeWithDynamicProps actual) {

        assertNotNull("Missing result", actual);
        assertFixedTestDataEquals(expected, actual);
        assertDynamicTestDataEquals(expected, actual);
    }

    public static void assertFixedTestDataEquals(
            TestDataTypeWithDynamicProps expected,
            TestDataTypeWithDynamicProps actual) {
        assertEquals(expected.getId(), actual.getId());

        assertEquals(expected.getFixedIssue(), actual.getFixedIssue());
        assertEquals(expected.getFixedLong(), actual.getFixedLong());
        assertEquals(expected.getFixedDocument(), actual.getFixedDocument());
        assertArrayEquals(expected.getFixedIssueArray(), actual.getFixedIssueArray());
        assertArrayEquals(expected.getFixedLongArray(), actual.getFixedLongArray());
        assertArrayEquals(expected.getFixedDocumentArray(), actual.getFixedDocumentArray());
        assertEquals(expected.getFixedIssueList(), actual.getFixedIssueList());
        assertEquals(expected.getFixedLongList(), actual.getFixedLongList());
        assertEquals(expected.getFixedDocumentList(), actual.getFixedDocumentList());
        assertEquals(expected.getFixedIssueSet(), actual.getFixedIssueSet());
        assertEquals(expected.getFixedLongSet(), actual.getFixedLongSet());
        assertEquals(expected.getFixedDocumentSet(), actual.getFixedDocumentSet());
        assertEquals(expected.getFixedMap(), actual.getFixedMap());
        assertEquals(expected.getFixedDocumentMap(), actual.getFixedDocumentMap());
    }

    public static void assertDynamicTestDataEquals(
            TestDataTypeWithDynamicProps expected,
            TestDataTypeWithDynamicProps actual) {
        assertEquals(expected.getDynamicIssue(), actual.getDynamicIssue());
        assertEquals(expected.getDynamicLong(), actual.getDynamicLong());
        assertEquals(expected.getDynamicDocument(), actual.getDynamicDocument());
        assertArrayEquals(expected.getDynamicIssueArray(), actual.getDynamicIssueArray());
        assertArrayEquals(expected.getDynamicLongArray(), actual.getDynamicLongArray());
        assertArrayEquals(expected.getDynamicDocumentArray(), actual.getDynamicDocumentArray());
        assertEquals(expected.getDynamicIssueList(), actual.getDynamicIssueList());
        assertEquals(expected.getDynamicLongList(), actual.getDynamicLongList());
        assertEquals(expected.getDynamicDocumentList(), actual.getDynamicDocumentList());
        assertEquals(expected.getDynamicIssueSet(), actual.getDynamicIssueSet());
        assertEquals(expected.getDynamicLongSet(), actual.getDynamicLongSet());
        assertEquals(expected.getDynamicDocumentSet(), actual.getDynamicDocumentSet());
        assertEquals(expected.getDynamicMap(), actual.getDynamicMap());
        assertEquals(expected.getDynamicDocumentMap(), actual.getDynamicDocumentMap());
    }

    public static void populateAllProperties(TestDataTypeWithDynamicProps data) {
        populateAllProperties(data, createSerializableIssuePojo(), getLong(), createDocument());
    }

    public static void populateFixedProperties(TestDataTypeWithDynamicProps data) {
        populateFixedProperties(data, createSerializableIssuePojo(), getLong(), createDocument());
    }

    public static void populateDynamicProperties(TestDataTypeWithDynamicProps data) {
        populateDynamicProperties(data, createSerializableIssuePojo(), getLong(), createDocument());
    }

    public static void populateAllProperties(
            TestDataTypeWithDynamicProps data,
            SerializableIssuePojo pojo,
            Long num,
            SpaceDocument doc) {
        populateFixedProperties(data, pojo, num, doc);
        populateDynamicProperties(data, pojo, num, doc);
    }

    public static void populateFixedProperties(
            TestDataTypeWithDynamicProps data,
            SerializableIssuePojo pojo,
            Long num,
            SpaceDocument doc) {
//        data.setId("" + random.nextLong());

        data.setFixedIssue(pojo);
        data.setFixedLong(num);
        data.setFixedDocument(doc);
        data.setFixedIssueArray(new SerializableIssuePojo[]{pojo});
        data.setFixedLongArray(new Long[]{num});
        data.setFixedDocumentArray(new SpaceDocument[]{doc});
        data.setFixedIssueList(list(pojo));
        data.setFixedLongList(list(num));
        data.setFixedDocumentList(list(doc));
        data.setFixedIssueSet(set(pojo));
        data.setFixedLongSet(set(num));
        data.setFixedDocumentSet(set(doc));
        data.setFixedMap(map(num, pojo));
        data.setFixedDocumentMap(map(num, doc));
    }

    public static void populateDynamicProperties(
            TestDataTypeWithDynamicProps data,
            SerializableIssuePojo pojo,
            Long num,
            SpaceDocument doc) {
        data.setDynamicIssue(pojo);
        data.setDynamicLong(num);
        data.setDynamicDocument(doc);
        data.setDynamicIssueArray(new SerializableIssuePojo[]{pojo});
        data.setDynamicLongArray(new Long[]{num});
        data.setDynamicDocumentArray(new SpaceDocument[]{doc});
        data.setDynamicIssueList(list(pojo));
        data.setDynamicLongList(list(num));
        data.setDynamicDocumentList(list(doc));
        data.setDynamicIssueSet(set(pojo));
        data.setDynamicLongSet(set(num));
        data.setDynamicDocumentSet(set(doc));
        data.setDynamicMap(map(num, pojo));
        data.setDynamicDocumentMap(map(num, doc));
    }

    private static ArrayList list(Object obj) {
        ArrayList result = new ArrayList();
        result.add(obj);
        return result;
    }

    private static Set set(Object obj) {
        Set set = new HashSet();
        set.add(obj);
        return set;
    }

    private static Map map(Object key, Object value) {
        Map map = new HashMap();
        map.put(key, value);
        return map;
    }

    private static long getLong() {
        return 10l;
    }

    private static SpaceDocument createDocument() {
        return new SpaceDocument("SomeTypeName")
                .setProperty("name", "dank")
                .setProperty("age", 23232);
    }

    private static SerializableIssuePojo createSerializableIssuePojo() {
        return new SerializableIssuePojo(1, "dankdank");
    }

}
