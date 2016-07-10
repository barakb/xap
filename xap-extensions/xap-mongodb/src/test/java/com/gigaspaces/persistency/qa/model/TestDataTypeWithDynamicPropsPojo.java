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

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceDynamicProperties;
import com.gigaspaces.annotation.pojo.SpaceExclude;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceDocumentSupport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("unchecked")
@SpaceClass
public class TestDataTypeWithDynamicPropsPojo implements TestDataTypeWithDynamicProps {

    private String id;
    private Map<String, Object> dynamicProps;
    private SerializableIssuePojo fixedIssue;
    private Long fixedLong;
    private SpaceDocument fixedDocument;
    private SerializableIssuePojo[] fixedIssueArray;
    private Long[] fixedLongArray;
    private SpaceDocument[] fixedDocumentArray;
    private ArrayList<SerializableIssuePojo> fixedIssueList;
    private ArrayList<Long> fixedLongList;
    private ArrayList<SpaceDocument> fixedDocumentList;
    private Set<SerializableIssuePojo> fixedIssueSet;
    private Set<Long> fixedLongSet;
    private Set<SpaceDocument> fixedDocumentSet;
    private Map<Long, SerializableIssuePojo> fixedMap;
    private Map<Long, SpaceDocument> fixedDocumentMap;

    public TestDataTypeWithDynamicPropsPojo() {

    }

    @SpaceId(autoGenerate = true)

    public String getId() {
        return id;
    }


    public void setId(String id) {
        this.id = id;
    }

    @SpaceDynamicProperties
    public Map<String, Object> getDynamicProps() {
        if (dynamicProps == null) {
            dynamicProps = new HashMap<String, Object>();
        }
        return dynamicProps;
    }

    public void setDynamicProps(Map<String, Object> dynamicProps) {
        this.dynamicProps = dynamicProps;
    }


    public SerializableIssuePojo getFixedIssue() {
        return fixedIssue;
    }


    public void setFixedIssue(SerializableIssuePojo fixedIssue) {
        this.fixedIssue = fixedIssue;
    }

    @SpaceExclude

    public SerializableIssuePojo getDynamicIssue() {
        return (SerializableIssuePojo) getDynamicProps().get("dynamicIssue");
    }


    public void setDynamicIssue(SerializableIssuePojo dynamicIssue) {
        getDynamicProps().put("dynamicIssue", dynamicIssue);
    }


    public Long getFixedLong() {
        return fixedLong;
    }


    public void setFixedLong(Long fixedLong) {
        this.fixedLong = fixedLong;
    }

    @SpaceExclude

    public Long getDynamicLong() {
        return (Long) getDynamicProps().get("dynamicLong");
    }


    public void setDynamicLong(Long dynamicLong) {
        getDynamicProps().put("dynamicLong", dynamicLong);
    }


    public SpaceDocument getFixedDocument() {
        return fixedDocument;
    }


    public void setFixedDocument(SpaceDocument fixedDocument) {
        this.fixedDocument = fixedDocument;
    }

    @SpaceExclude

    public SpaceDocument getDynamicDocument() {
        return (SpaceDocument) getDynamicProps().get("dynamicDocument");
    }


    public void setDynamicDocument(SpaceDocument dynamicDocument) {
        getDynamicProps().put("dynamicDocument", dynamicDocument);
    }


    public SerializableIssuePojo[] getFixedIssueArray() {
        return fixedIssueArray;
    }


    public void setFixedIssueArray(SerializableIssuePojo[] fixedIssueArray) {
        this.fixedIssueArray = fixedIssueArray;
    }

    @SpaceExclude

    public SerializableIssuePojo[] getDynamicIssueArray() {
        return (SerializableIssuePojo[]) getDynamicProps().get("dynamicIssueArray");
    }


    public void setDynamicIssueArray(SerializableIssuePojo[] dynamicIssueArray) {
        getDynamicProps().put("dynamicIssueArray", dynamicIssueArray);
    }


    public Long[] getFixedLongArray() {
        return fixedLongArray;
    }


    public void setFixedLongArray(Long[] fixedLongArray) {
        this.fixedLongArray = fixedLongArray;
    }

    @SpaceExclude

    public Long[] getDynamicLongArray() {
        return (Long[]) getDynamicProps().get("dynamicLongArray");
    }


    public void setDynamicLongArray(Long[] dynamicLongArray) {
        getDynamicProps().put("dynamicLongArray", dynamicLongArray);
    }


    public SpaceDocument[] getFixedDocumentArray() {
        return fixedDocumentArray;
    }


    public void setFixedDocumentArray(SpaceDocument[] fixedDocumentArray) {
        this.fixedDocumentArray = fixedDocumentArray;
    }

    @SpaceExclude

    public SpaceDocument[] getDynamicDocumentArray() {
        return (SpaceDocument[]) getDynamicProps().get("dynamicDocumentArray");
    }


    public void setDynamicDocumentArray(SpaceDocument[] dynamicDocumentArray) {
        getDynamicProps().put("dynamicDocumentArray", dynamicDocumentArray);
    }


    public ArrayList<SerializableIssuePojo> getFixedIssueList() {
        return fixedIssueList;
    }


    public void setFixedIssueList(ArrayList<SerializableIssuePojo> fixedIssueList) {
        this.fixedIssueList = fixedIssueList;
    }

    @SpaceExclude

    public ArrayList<SerializableIssuePojo> getDynamicIssueList() {
        return (ArrayList<SerializableIssuePojo>) getDynamicProps().get("dynamicIssueList");
    }


    public void setDynamicIssueList(ArrayList<SerializableIssuePojo> dynamicIssueList) {
        getDynamicProps().put("dynamicIssueList", dynamicIssueList);
    }


    public ArrayList<Long> getFixedLongList() {
        return fixedLongList;
    }


    public void setFixedLongList(ArrayList<Long> fixedLongList) {
        this.fixedLongList = fixedLongList;
    }

    @SpaceExclude

    public ArrayList<Long> getDynamicLongList() {
        return (ArrayList<Long>) getDynamicProps().get("dynamicLongList");
    }


    public void setDynamicLongList(ArrayList<Long> dynamicLongList) {
        getDynamicProps().put("dynamicLongList", dynamicLongList);
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.COPY)

    public ArrayList<SpaceDocument> getFixedDocumentList() {
        return fixedDocumentList;
    }


    public void setFixedDocumentList(ArrayList<SpaceDocument> fixedDocumentList) {
        this.fixedDocumentList = fixedDocumentList;
    }

    @SpaceExclude

    public ArrayList<SpaceDocument> getDynamicDocumentList() {
        return (ArrayList<SpaceDocument>) getDynamicProps().get("dynamicDocumentList");
    }


    public void setDynamicDocumentList(ArrayList<SpaceDocument> dynamicDocumentList) {
        getDynamicProps().put("dynamicDocumentList", dynamicDocumentList);
    }


    public Set<SerializableIssuePojo> getFixedIssueSet() {
        return fixedIssueSet;
    }


    public void setFixedIssueSet(Set<SerializableIssuePojo> fixedIssueSet) {
        this.fixedIssueSet = fixedIssueSet;
    }

    @SpaceExclude

    public Set<SerializableIssuePojo> getDynamicIssueSet() {
        return (Set<SerializableIssuePojo>) getDynamicProps().get("dynamicIssueSet");
    }


    public void setDynamicIssueSet(Set<SerializableIssuePojo> dynamicIssueSet) {
        getDynamicProps().put("dynamicIssueSet", dynamicIssueSet);
    }


    public Set<Long> getFixedLongSet() {
        return fixedLongSet;
    }


    public void setFixedLongSet(Set<Long> fixedLongSet) {
        this.fixedLongSet = fixedLongSet;
    }

    @SpaceExclude

    public Set<Long> getDynamicLongSet() {
        return (Set<Long>) getDynamicProps().get("dynamicLongSet");
    }


    public void setDynamicLongSet(Set<Long> dynamicLongSet) {
        getDynamicProps().put("dynamicLongSet", dynamicLongSet);
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.COPY)

    public Set<SpaceDocument> getFixedDocumentSet() {
        return fixedDocumentSet;
    }


    public void setFixedDocumentSet(Set<SpaceDocument> fixedDocumentSet) {
        this.fixedDocumentSet = fixedDocumentSet;
    }

    @SpaceExclude

    public Set<SpaceDocument> getDynamicDocumentSet() {
        return (Set<SpaceDocument>) getDynamicProps().get("dynamicDocumentSet");
    }


    public void setDynamicDocumentSet(Set<SpaceDocument> dynamicDocumentSet) {
        getDynamicProps().put("dynamicDocumentSet", dynamicDocumentSet);
    }


    public Map<Long, SerializableIssuePojo> getFixedMap() {
        return fixedMap;
    }


    public void setFixedMap(Map<Long, SerializableIssuePojo> fixedMap) {
        this.fixedMap = fixedMap;
    }

    @SpaceExclude

    public Map<Long, SerializableIssuePojo> getDynamicMap() {
        return (Map<Long, SerializableIssuePojo>) getDynamicProps().get("dynamicMap");
    }


    public void setDynamicMap(Map<Long, SerializableIssuePojo> dynamicMap) {
        getDynamicProps().put("dynamicMap", dynamicMap);
    }


    public Map<Long, SpaceDocument> getFixedDocumentMap() {
        return fixedDocumentMap;
    }


    public void setFixedDocumentMap(Map<Long, SpaceDocument> fixedDocumentMap) {
        this.fixedDocumentMap = fixedDocumentMap;
    }

    @SpaceExclude

    public Map<Long, SpaceDocument> getDynamicDocumentMap() {
        return (Map<Long, SpaceDocument>) getDynamicProps().get("dynamicDocumentMap");
    }


    public void setDynamicDocumentMap(Map<Long, SpaceDocument> dynamicDocumentMap) {
        getDynamicProps().put("dynamicDocumentMap", dynamicDocumentMap);
    }

}
