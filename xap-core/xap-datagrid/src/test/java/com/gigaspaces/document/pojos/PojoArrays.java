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
import com.gigaspaces.metadata.SpaceDocumentSupport;

import java.util.Arrays;

@com.gigaspaces.api.InternalApi
public class PojoArrays {
    private byte[] byteArray;
    private short[] shortArray;
    private int[] intArray;
    private long[] longArray;
    private float[] floatArray;
    private double[] doubleArray;
    private boolean[] booleanArray;
    private char[] charArray;
    private Byte[] byteWrapperArray;
    private Short[] shortWrapperArray;
    private Integer[] intWrapperArray;
    private Long[] longWrapperArray;
    private Float[] floatWrapperArray;
    private Double[] doubleWrapperArray;
    private Boolean[] booleanWrapperArray;
    private Character[] charWrapperArray;
    private Object[] objectArray;
    private Person[] personArray1;
    private Person[] personArray2;

    public byte[] getByteArray() {
        return byteArray;
    }

    public void setByteArray(byte[] array) {
        this.byteArray = array;
    }

    public short[] getShortArray() {
        return shortArray;
    }

    public void setShortArray(short[] array) {
        this.shortArray = array;
    }

    public int[] getIntArray() {
        return intArray;
    }

    public void setIntArray(int[] array) {
        this.intArray = array;
    }

    public long[] getLongArray() {
        return longArray;
    }

    public void setLongArray(long[] array) {
        this.longArray = array;
    }

    public float[] getFloatArray() {
        return floatArray;
    }

    public void setFloatArray(float[] array) {
        this.floatArray = array;
    }

    public double[] getDoubleArray() {
        return doubleArray;
    }

    public void setDoubleArray(double[] array) {
        this.doubleArray = array;
    }

    public boolean[] getBooleanArray() {
        return booleanArray;
    }

    public void setBooleanArray(boolean[] array) {
        this.booleanArray = array;
    }

    public char[] getCharArray() {
        return charArray;
    }

    public void setCharArray(char[] array) {
        this.charArray = array;
    }

    public Byte[] getByteWrapperArray() {
        return byteWrapperArray;
    }

    public void setByteWrapperArray(Byte[] array) {
        this.byteWrapperArray = array;
    }

    public Short[] getShortWrapperArray() {
        return shortWrapperArray;
    }

    public void setShortWrapperArray(Short[] array) {
        this.shortWrapperArray = array;
    }

    public Integer[] getIntWrapperArray() {
        return intWrapperArray;
    }

    public void setIntWrapperArray(Integer[] array) {
        this.intWrapperArray = array;
    }

    public Long[] getLongWrapperArray() {
        return longWrapperArray;
    }

    public void setLongWrapperArray(Long[] array) {
        this.longWrapperArray = array;
    }

    public Float[] getFloatWrapperArray() {
        return floatWrapperArray;
    }

    public void setFloatWrapperArray(Float[] array) {
        this.floatWrapperArray = array;
    }

    public Double[] getDoubleWrapperArray() {
        return doubleWrapperArray;
    }

    public void setDoubleWrapperArray(Double[] array) {
        this.doubleWrapperArray = array;
    }

    public Boolean[] getBooleanWrapperArray() {
        return booleanWrapperArray;
    }

    public void setBooleanWrapperArray(Boolean[] array) {
        this.booleanWrapperArray = array;
    }

    public Character[] getCharWrapperArray() {
        return charWrapperArray;
    }

    public void setCharWrapperArray(Character[] array) {
        this.charWrapperArray = array;
    }

    public Object[] getObjectArray() {
        return objectArray;
    }

    public void setObjectArray(Object[] array) {
        this.objectArray = array;
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.CONVERT)
    public Person[] getPersonArray1() {
        return personArray1;
    }

    public void setPersonArray1(Person[] array) {
        this.personArray1 = array;
    }

    @SpaceProperty(documentSupport = SpaceDocumentSupport.COPY)
    public Person[] getPersonArray2() {
        return personArray2;
    }

    public void setPersonArray2(Person[] array) {
        this.personArray2 = array;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (!(obj instanceof PojoArrays))
            return false;

        PojoArrays other = (PojoArrays) obj;
        if (!Arrays.equals(this.byteArray, other.byteArray))
            return false;
        if (!Arrays.equals(this.shortArray, other.shortArray))
            return false;
        if (!Arrays.equals(this.intArray, other.intArray))
            return false;
        if (!Arrays.equals(this.longArray, other.longArray))
            return false;
        if (!Arrays.equals(this.floatArray, other.floatArray))
            return false;
        if (!Arrays.equals(this.doubleArray, other.doubleArray))
            return false;
        if (!Arrays.equals(this.booleanArray, other.booleanArray))
            return false;
        if (!Arrays.equals(this.charArray, other.charArray))
            return false;
        if (!Arrays.equals(this.byteWrapperArray, other.byteWrapperArray))
            return false;
        if (!Arrays.equals(this.shortWrapperArray, other.shortWrapperArray))
            return false;
        if (!Arrays.equals(this.intWrapperArray, other.intWrapperArray))
            return false;
        if (!Arrays.equals(this.longWrapperArray, other.longWrapperArray))
            return false;
        if (!Arrays.equals(this.floatWrapperArray, other.floatWrapperArray))
            return false;
        if (!Arrays.equals(this.doubleWrapperArray, other.doubleWrapperArray))
            return false;
        if (!Arrays.equals(this.booleanWrapperArray, other.booleanWrapperArray))
            return false;
        if (!Arrays.equals(this.charWrapperArray, other.charWrapperArray))
            return false;
        if (!Arrays.equals(this.objectArray, other.objectArray))
            return false;
        if (!Arrays.equals(this.personArray1, other.personArray1))
            return false;
        if (!Arrays.equals(this.personArray2, other.personArray2))
            return false;

        return true;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" +
                "ByteArray=" + Arrays.toString(byteArray) +
                "ShortArray=" + Arrays.toString(shortArray) +
                "IntArray=" + Arrays.toString(intArray) +
                "LongArray=" + Arrays.toString(longArray) +
                "FloatArray=" + Arrays.toString(floatArray) +
                "DoubleArray=" + Arrays.toString(doubleArray) +
                "BooleanArray=" + Arrays.toString(booleanArray) +
                "CharArray=" + Arrays.toString(charArray) +
                "ByteWrapperArray=" + Arrays.toString(byteWrapperArray) +
                "ShortWrapperArray=" + Arrays.toString(shortWrapperArray) +
                "IntWrapperArray=" + Arrays.toString(intWrapperArray) +
                "LongWrapperArray=" + Arrays.toString(longWrapperArray) +
                "FloatWrapperArray=" + Arrays.toString(floatWrapperArray) +
                "DoubleWrapperArray=" + Arrays.toString(doubleWrapperArray) +
                "BooleanWrapperArray=" + Arrays.toString(booleanWrapperArray) +
                "CharWrapperArray=" + Arrays.toString(charWrapperArray) +
                "objectArray=" + objectArray +
                "personArray1=" + personArray1 +
                "personArray2=" + personArray2 +
                "]";
    }
}
