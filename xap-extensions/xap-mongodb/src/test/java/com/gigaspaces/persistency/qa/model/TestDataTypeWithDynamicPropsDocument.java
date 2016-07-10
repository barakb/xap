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
import java.util.Map;
import java.util.Set;

public class TestDataTypeWithDynamicPropsDocument extends SpaceDocument
        implements TestDataTypeWithDynamicProps {

    private final String id = "id";
    private final String fixedIssue = "fixedIssue";
    private final String dynamicIssue = "dynamicIssue";
    private final String fixedLong = "fixedLong";
    private final String dynamicLong = "dynamicLong";
    private final String fixedDocument = "fixedDocument";
    private final String dynamicDocument = "dynamicDocument";
    private final String fixedIssueArray = "fixedIssueArray";
    private final String dynamicIssueArray = "dynamicIssueArray";
    private final String fixedLongArray = "fixedLongArray";
    private final String dynamicLongArray = "dynamicLongArray";
    private final String fixedDocumentArray = "fixedDocumentArray";
    private final String dynamicDocumentArray = "dynamicDocumentArray";
    private final String fixedIssueList = "fixedIssueList";
    private final String dynamicIssueList = "dynamicIssueList";
    private final String fixedLongList = "fixedLongList";
    private final String dynamicLongList = "dynamicLongList";
    private final String fixedDocumentList = "fixedDocumentList";
    private final String dynamicDocumentList = "dynamicDocumentList";
    private final String fixedIssueSet = "fixedIssueSet";
    private final String dynamicIssueSet = "dynamicIssueSet";
    private final String fixedLongSet = "fixedLongSet";
    private final String dynamicLongSet = "dynamicLongSet";
    private final String fixedDocumentSet = "fixedDocumentSet";
    private final String dynamicDocumentSet = "dynamicDocumentSet";
    private final String fixedMap = "fixedMap";
    private final String dynamicMap = "dynamicMap";
    private final String fixedDocumentMap = "fixedDocumentMap";
    private final String dynamicDocumentMap = "dynamicDocumentMap";

    public TestDataTypeWithDynamicPropsDocument() {
        this(null);
    }

    public TestDataTypeWithDynamicPropsDocument(SpaceDocument from) {
        setTypeName(TestDataTypeWithDynamicPropsPojo.class.getName());
        if (from != null) {
            addProperties(from.getProperties());
        }
    }

    public String getId() {
        return getProperty(id);
    }

    public void setId(String id) {
        setProperty(this.id, id);
    }

    public SerializableIssuePojo getFixedIssue() {
        return getProperty(fixedIssue);
    }

    public void setFixedIssue(SerializableIssuePojo fixedIssue) {
        setProperty(this.fixedIssue, fixedIssue);
    }

    public SerializableIssuePojo getDynamicIssue() {
        return getProperty(dynamicIssue);
    }

    public void setDynamicIssue(SerializableIssuePojo dynamicIssue) {
        setProperty(this.dynamicIssue, dynamicIssue);
    }

    public Long getFixedLong() {
        return getProperty(fixedLong);
    }

    public void setFixedLong(Long fixedLong) {
        setProperty(this.fixedLong, fixedLong);
    }

    public Long getDynamicLong() {
        return getProperty(dynamicLong);
    }

    public void setDynamicLong(Long dynamicLong) {
        setProperty(this.dynamicLong, dynamicLong);
    }

    public SpaceDocument getFixedDocument() {
        return getProperty(fixedDocument);
    }

    public void setFixedDocument(SpaceDocument fixedDocument) {
        setProperty(this.fixedDocument, fixedDocument);
    }

    public SpaceDocument getDynamicDocument() {
        return getProperty(dynamicDocument);
    }

    public void setDynamicDocument(SpaceDocument dynamicDocument) {
        setProperty(this.dynamicDocument, dynamicDocument);
    }

    public SerializableIssuePojo[] getFixedIssueArray() {
        return getProperty(fixedIssueArray);
    }

    public void setFixedIssueArray(SerializableIssuePojo[] fixedIssueArray) {
        setProperty(this.fixedIssueArray, fixedIssueArray);
    }

    public SerializableIssuePojo[] getDynamicIssueArray() {
        return getProperty(dynamicIssueArray);
    }

    public void setDynamicIssueArray(SerializableIssuePojo[] dynamicIssueArray) {
        setProperty(this.dynamicIssueArray, dynamicIssueArray);
    }

    public Long[] getFixedLongArray() {
        return getProperty(fixedLongArray);
    }

    public void setFixedLongArray(Long[] fixedLongArray) {
        setProperty(this.fixedLongArray, fixedLongArray);
    }

    public Long[] getDynamicLongArray() {
        return getProperty(dynamicLongArray);
    }

    public void setDynamicLongArray(Long[] dynamicLongArray) {
        setProperty(this.dynamicLongArray, dynamicLongArray);
    }

    public SpaceDocument[] getFixedDocumentArray() {
        return getProperty(fixedDocumentArray);
    }

    public void setFixedDocumentArray(SpaceDocument[] fixedDocumentArray) {
        setProperty(this.fixedDocumentArray, fixedDocumentArray);
    }

    public SpaceDocument[] getDynamicDocumentArray() {
        return getProperty(dynamicDocumentArray);
    }

    public void setDynamicDocumentArray(SpaceDocument[] dynamicDocumentArray) {
        setProperty(this.dynamicDocumentArray, dynamicDocumentArray);
    }

    public ArrayList<SerializableIssuePojo> getFixedIssueList() {
        return getProperty(fixedIssueList);
    }

    public void setFixedIssueList(
            ArrayList<SerializableIssuePojo> fixedIssueList) {
        setProperty(this.fixedIssueList, fixedIssueList);
    }

    public ArrayList<SerializableIssuePojo> getDynamicIssueList() {
        return getProperty(dynamicIssueList);
    }

    public void setDynamicIssueList(
            ArrayList<SerializableIssuePojo> dynamicIssueList) {
        setProperty(this.dynamicIssueList, dynamicIssueList);
    }

    public ArrayList<Long> getFixedLongList() {
        return getProperty(fixedLongList);
    }

    public void setFixedLongList(ArrayList<Long> fixedLongList) {
        setProperty(this.fixedLongList, fixedLongList);
    }

    public ArrayList<Long> getDynamicLongList() {
        return getProperty(dynamicLongList);
    }

    public void setDynamicLongList(ArrayList<Long> dynamicLongList) {
        setProperty(this.dynamicLongList, dynamicLongList);
    }

    public ArrayList<SpaceDocument> getFixedDocumentList() {
        return getProperty(fixedDocumentList);
    }

    public void setFixedDocumentList(ArrayList<SpaceDocument> fixedDocumentList) {
        setProperty(this.fixedDocumentList, fixedDocumentList);
    }

    public ArrayList<SpaceDocument> getDynamicDocumentList() {
        return getProperty(dynamicDocumentList);
    }

    public void setDynamicDocumentList(
            ArrayList<SpaceDocument> dynamicDocumentList) {
        setProperty(this.dynamicDocumentList, dynamicDocumentList);
    }

    public Set<SerializableIssuePojo> getFixedIssueSet() {
        return getProperty(fixedIssueSet);
    }

    public void setFixedIssueSet(Set<SerializableIssuePojo> fixedIssueSet) {
        setProperty(this.fixedIssueSet, fixedIssueSet);
    }

    public Set<SerializableIssuePojo> getDynamicIssueSet() {
        return getProperty(dynamicIssueSet);
    }

    public void setDynamicIssueSet(Set<SerializableIssuePojo> dynamicIssueSet) {
        setProperty(this.dynamicIssueSet, dynamicIssueSet);
    }

    public Set<Long> getFixedLongSet() {
        return getProperty(fixedLongSet);
    }

    public void setFixedLongSet(Set<Long> fixedLongSet) {
        setProperty(this.fixedLongSet, fixedLongSet);
    }

    public Set<Long> getDynamicLongSet() {
        return getProperty(dynamicLongSet);
    }

    public void setDynamicLongSet(Set<Long> dynamicLongSet) {
        setProperty(this.dynamicLongSet, dynamicLongSet);
    }

    public Set<SpaceDocument> getFixedDocumentSet() {
        return getProperty(fixedDocumentSet);
    }

    public void setFixedDocumentSet(Set<SpaceDocument> fixedDocumentSet) {
        setProperty(this.fixedDocumentSet, fixedDocumentSet);
    }

    public Set<SpaceDocument> getDynamicDocumentSet() {
        return getProperty(dynamicDocumentSet);
    }

    public void setDynamicDocumentSet(Set<SpaceDocument> dynamicDocumentSet) {
        setProperty(this.dynamicDocumentSet, dynamicDocumentSet);
    }

    public Map<Long, SerializableIssuePojo> getFixedMap() {
        return getProperty(fixedMap);
    }

    public void setFixedMap(Map<Long, SerializableIssuePojo> fixedMap) {
        setProperty(this.fixedMap, fixedMap);
    }

    public Map<Long, SerializableIssuePojo> getDynamicMap() {
        return getProperty(dynamicMap);
    }

    public void setDynamicMap(Map<Long, SerializableIssuePojo> dynamicMap) {
        setProperty(this.dynamicMap, dynamicMap);
    }

    public Map<Long, SpaceDocument> getFixedDocumentMap() {
        return getProperty(fixedDocumentMap);
    }

    public void setFixedDocumentMap(Map<Long, SpaceDocument> fixedDocumentMap) {
        setProperty(this.fixedDocumentMap, fixedDocumentMap);
    }

    public Map<Long, SpaceDocument> getDynamicDocumentMap() {
        return getProperty(dynamicDocumentMap);
    }

    public void setDynamicDocumentMap(
            Map<Long, SpaceDocument> dynamicDocumentMap) {
        setProperty(this.dynamicDocumentMap, dynamicDocumentMap);
    }

}
