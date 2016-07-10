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

package com.gigaspaces.metadata.annotated;

import com.gigaspaces.annotation.pojo.SpaceProperty;

@com.gigaspaces.api.InternalApi
public class PojoCommonProperties {
    private String _someString;
    private Object _someObject;
    private boolean _someBoolean;
    private Boolean _someBooleanWrapper;
    private int _someInt;
    private Integer _someIntWrapper;
    private long _someLong;
    private Long _someLongWrapper;
    private String _someOverload;

    public String getSomeString() {
        return _someString;
    }

    public void setSomeString(String someString) {
        this._someString = someString;
    }

    public Object getSomeObject() {
        return _someObject;
    }

    public void setSomeObject(Object someString) {
        this._someObject = someString;
    }

    @SpaceProperty(nullValue = "true")
    public boolean isSomeBoolean() {
        return _someBoolean;
    }

    public void setSomeBoolean(boolean someBoolean) {
        this._someBoolean = someBoolean;
    }

    public Boolean getSomeBooleanWrapper() {
        return _someBooleanWrapper;
    }

    public void setSomeBooleanWrapper(Boolean someBooleanWrapper) {
        this._someBooleanWrapper = someBooleanWrapper;
    }

    @SpaceProperty(nullValue = "-1")
    public int getSomeInt() {
        return _someInt;
    }

    public void setSomeInt(int someInt) {
        this._someInt = someInt;
    }

    public Integer getSomeIntWrapper() {
        return _someIntWrapper;
    }

    public void setSomeIntWrapper(Integer someIntWrapper) {
        this._someIntWrapper = someIntWrapper;
    }

    @SpaceProperty(nullValue = "-10")
    public long getSomeLong() {
        return _someLong;
    }

    public void setSomeLong(long someLong) {
        this._someLong = someLong;
    }

    public Long getSomeLongWrapper() {
        return _someLongWrapper;
    }

    public void setSomeLongWrapper(Long someLongWrapper) {
        this._someLongWrapper = someLongWrapper;
    }

    public String getSomeOverload() {
        return _someOverload;
    }

    public void setSomeOverload(String s) {
        this._someOverload = s;
    }

    public void setSomeOverload(int i) {
        this._someOverload = "The number " + i;
    }

    public int getSomeReadonlyProperty() {
        return -1;
    }

    public void setSomeWriteonlyProperty(int value) {

    }
}
