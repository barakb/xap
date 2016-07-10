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

package com.gigaspaces.document.pojos;

import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.metadata.SpaceDocumentSupport;

import java.util.List;

@com.gigaspaces.api.InternalApi
public class PojoCollections {
    private List<Byte> byteList;
    private List<Short> shortList;
    private List<Integer> intList;
    private List<Long> longList;
    private List<Float> floatList;
    private List<Double> doubleList;
    private List<Boolean> booleanList;
    private List<Character> charList;
    private List<Object> objectList;
    private List<Person> personList1;
    private List<Person> personList2;

    public List<Byte> getByteList() {
        return byteList;
    }

    public void setByteList(List<Byte> list) {
        this.byteList = list;
    }

    public List<Short> getShortList() {
        return shortList;
    }

    public void setShortList(List<Short> list) {
        this.shortList = list;
    }

    public List<Integer> getIntList() {
        return intList;
    }

    public void setIntList(List<Integer> list) {
        this.intList = list;
    }

    public List<Long> getLongList() {
        return longList;
    }

    public void setLongList(List<Long> list) {
        this.longList = list;
    }

    public List<Float> getFloatList() {
        return floatList;
    }

    public void setFloatList(List<Float> list) {
        this.floatList = list;
    }

    public List<Double> getDoubleList() {
        return doubleList;
    }

    public void setDoubleList(List<Double> list) {
        this.doubleList = list;
    }

    public List<Boolean> getBooleanList() {
        return booleanList;
    }

    public void setBooleanList(List<Boolean> list) {
        this.booleanList = list;
    }

    public List<Character> getCharList() {
        return charList;
    }

    public void setCharList(List<Character> list) {
        this.charList = list;
    }

    public List<Object> getObjectList() {
        return objectList;
    }

    public void setObjectList(List<Object> list) {
        this.objectList = list;
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.CONVERT)
    public List<Person> getPersonList1() {
        return personList1;
    }

    public void setPersonList1(List<Person> list) {
        this.personList1 = list;
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.COPY)
    public List<Person> getPersonList2() {
        return personList2;
    }

    public void setPersonList2(List<Person> list) {
        this.personList2 = list;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (!(obj instanceof PojoCollections))
            return false;

        PojoCollections other = (PojoCollections) obj;
        if (!ObjectUtils.equals(this.byteList, other.byteList))
            return false;
        if (!ObjectUtils.equals(this.shortList, other.shortList))
            return false;
        if (!ObjectUtils.equals(this.intList, other.intList))
            return false;
        if (!ObjectUtils.equals(this.longList, other.longList))
            return false;
        if (!ObjectUtils.equals(this.floatList, other.floatList))
            return false;
        if (!ObjectUtils.equals(this.doubleList, other.doubleList))
            return false;
        if (!ObjectUtils.equals(this.booleanList, other.booleanList))
            return false;
        if (!ObjectUtils.equals(this.charList, other.charList))
            return false;
        if (!ObjectUtils.equals(this.objectList, other.objectList))
            return false;
        if (!ObjectUtils.equals(this.personList1, other.personList1))
            return false;
        if (!ObjectUtils.equals(this.personList2, other.personList2))
            return false;

        return true;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" +
                "propByte=" + byteList +
                "propShort=" + shortList +
                "propInt=" + intList +
                "propLong=" + longList +
                "propFloat=" + floatList +
                "propDouble=" + doubleList +
                "propBoolean=" + booleanList +
                "propChar=" + charList +
                "objectList=" + objectList +
                "personList1=" + personList1 +
                "personList2=" + personList2 +
                "]";
    }
}
