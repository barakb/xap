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

import com.gigaspaces.annotation.pojo.SpaceExclude;
import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.annotation.pojo.SpaceProperty.IndexType;

@com.gigaspaces.api.InternalApi
public class PojoInheritenceSort1 {
    private String _fieldB;
    private String _fieldD;

    // Read\write property
    public String getFieldB() {
        return _fieldB;
    }

    public void setFieldB(String fieldB) {
        _fieldB = fieldB;
    }

    // Read only property
    public String getFieldB1() {
        return _fieldB;
    }

    // Write only property
    public void setFieldB2(String fieldB) {
        _fieldB = fieldB;
    }

    // property for no override test.
    public String getFieldD() {
        return _fieldD;
    }

    public void setFieldD(String fieldD) {
        _fieldD = fieldD;
    }

    // property getter & setter override test.
    public String getFieldD1() {
        return _fieldD;
    }

    public void setFieldD1(String fieldD) {
        _fieldD = fieldD;
    }

    // property getter override test.
    public String getFieldD2() {
        return _fieldD;
    }

    public void setFieldD2(String fieldD) {
        _fieldD = fieldD;
    }

    // property setter override test.
    public String getFieldD3() {
        return _fieldD;
    }

    public void setFieldD3(String fieldD) {
        _fieldD = fieldD;
    }

    // Property for index override test:
    @SpaceProperty(index = IndexType.NONE)
    public String getFieldD4() {
        return _fieldD;
    }

    public void setFieldD4(String fieldD) {
        _fieldD = fieldD;
    }

    // Property for exclude override test.
    @SpaceExclude
    public String getFieldD5() {
        return _fieldD;
    }

    public void setFieldD5(String fieldD) {
        _fieldD = fieldD;
    }

    // Property for index downgrade test.
    @SpaceProperty(index = IndexType.BASIC)
    public String getFieldD6() {
        return _fieldD;
    }

    public void setFieldD6(String fieldD) {
        _fieldD = fieldD;
    }
}
