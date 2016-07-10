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

import com.gigaspaces.internal.utils.ObjectUtils;

@com.gigaspaces.api.InternalApi
public class PojoCommonTypes {
    private byte propByte;
    private short propShort;
    private int propInt;
    private long propLong;
    private float propFloat;
    private double propDouble;
    private boolean propBoolean;
    private char propChar;
    private Byte propByteWrapper;
    private Short propShortWrapper;
    private Integer propIntWrapper;
    private Long propLongWrapper;
    private Float propFloatWrapper;
    private Double propDoubleWrapper;
    private Boolean propBooleanWrapper;
    private Character propCharWrapper;

    public byte getPropByte() {
        return propByte;
    }

    public void setPropByte(byte propByte) {
        this.propByte = propByte;
    }

    public short getPropShort() {
        return propShort;
    }

    public void setPropShort(short propShort) {
        this.propShort = propShort;
    }

    public int getPropInt() {
        return propInt;
    }

    public void setPropInt(int propInt) {
        this.propInt = propInt;
    }

    public long getPropLong() {
        return propLong;
    }

    public void setPropLong(long propLong) {
        this.propLong = propLong;
    }

    public float getPropFloat() {
        return propFloat;
    }

    public void setPropFloat(float propFloat) {
        this.propFloat = propFloat;
    }

    public double getPropDouble() {
        return propDouble;
    }

    public void setPropDouble(double propDouble) {
        this.propDouble = propDouble;
    }

    public boolean getPropBoolean() {
        return propBoolean;
    }

    public void setPropBoolean(boolean propBoolean) {
        this.propBoolean = propBoolean;
    }

    public char getPropChar() {
        return propChar;
    }

    public void setPropChar(char propChar) {
        this.propChar = propChar;
    }

    public Byte getPropByteWrapper() {
        return propByteWrapper;
    }

    public void setPropByteWrapper(Byte propByte) {
        this.propByteWrapper = propByte;
    }

    public Short getPropShortWrapper() {
        return propShortWrapper;
    }

    public void setPropShortWrapper(Short propShort) {
        this.propShortWrapper = propShort;
    }

    public Integer getPropIntWrapper() {
        return propIntWrapper;
    }

    public void setPropIntWrapper(Integer propInt) {
        this.propIntWrapper = propInt;
    }

    public Long getPropLongWrapper() {
        return propLongWrapper;
    }

    public void setPropLongWrapper(Long propLong) {
        this.propLongWrapper = propLong;
    }

    public Float getPropFloatWrapper() {
        return propFloatWrapper;
    }

    public void setPropFloatWrapper(Float propFloat) {
        this.propFloatWrapper = propFloat;
    }

    public Double getPropDoubleWrapper() {
        return propDoubleWrapper;
    }

    public void setPropDoubleWrapper(Double propDouble) {
        this.propDoubleWrapper = propDouble;
    }

    public Boolean getPropBooleanWrapper() {
        return propBooleanWrapper;
    }

    public void setPropBooleanWrapper(Boolean propBoolean) {
        this.propBooleanWrapper = propBoolean;
    }

    public Character getPropCharWrapper() {
        return propCharWrapper;
    }

    public void setPropCharWrapper(Character propChar) {
        this.propCharWrapper = propChar;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (!(obj instanceof PojoCommonTypes))
            return false;

        PojoCommonTypes other = (PojoCommonTypes) obj;
        if (!ObjectUtils.equals(this.propByte, other.propByte))
            return false;
        if (!ObjectUtils.equals(this.propShort, other.propShort))
            return false;
        if (!ObjectUtils.equals(this.propInt, other.propInt))
            return false;
        if (!ObjectUtils.equals(this.propLong, other.propLong))
            return false;
        if (!ObjectUtils.equals(this.propFloat, other.propFloat))
            return false;
        if (!ObjectUtils.equals(this.propDouble, other.propDouble))
            return false;
        if (!ObjectUtils.equals(this.propBoolean, other.propBoolean))
            return false;
        if (!ObjectUtils.equals(this.propChar, other.propChar))
            return false;
        if (!ObjectUtils.equals(this.propByteWrapper, other.propByteWrapper))
            return false;
        if (!ObjectUtils.equals(this.propShortWrapper, other.propShortWrapper))
            return false;
        if (!ObjectUtils.equals(this.propIntWrapper, other.propIntWrapper))
            return false;
        if (!ObjectUtils.equals(this.propLongWrapper, other.propLongWrapper))
            return false;
        if (!ObjectUtils.equals(this.propFloatWrapper, other.propFloatWrapper))
            return false;
        if (!ObjectUtils.equals(this.propDoubleWrapper, other.propDoubleWrapper))
            return false;
        if (!ObjectUtils.equals(this.propBooleanWrapper, other.propBooleanWrapper))
            return false;
        if (!ObjectUtils.equals(this.propCharWrapper, other.propCharWrapper))
            return false;

        return true;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" +
                "propByte=" + propByte +
                "propShort=" + propShort +
                "propInt=" + propInt +
                "propLong=" + propLong +
                "propFloat=" + propFloat +
                "propDouble=" + propDouble +
                "propBoolean=" + propBoolean +
                "propChar=" + propChar +
                "propByteWrapper=" + propByteWrapper +
                "propShortWrapper=" + propShortWrapper +
                "propIntWrapper=" + propIntWrapper +
                "propLongWrapper=" + propLongWrapper +
                "propFloatWrapper=" + propFloatWrapper +
                "propDoubleWrapper=" + propDoubleWrapper +
                "propBooleanWrapper=" + propBooleanWrapper +
                "propCharWrapper=" + propCharWrapper +
                "]";
    }
}
