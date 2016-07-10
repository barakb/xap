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
import com.gigaspaces.annotation.pojo.SpaceProperty.IndexType;

@com.gigaspaces.api.InternalApi
public class PojoInheritenceSort2 extends PojoInheritenceSort1 {
    private String _fieldA;
    private String _fieldC;
    private String _fieldE;

    // Add 'missing' setter from read only property:
    public void setFieldB1(String fieldB) {
    }

    // Add 'missing' getter from Write only property:
    public String getFieldB2() {
        return null;
    }

    // Override both getter and setter
    @Override
    public String getFieldD1() {
        return null;
    }

    @Override
    public void setFieldD1(String fieldD) {
    }

    // Override getter only
    @Override
    public String getFieldD2() {
        return null;
    }

    // Override setter only
    @Override
    public void setFieldD3(String fieldD) {
    }

    // Override and change index:
    @Override
    @SpaceProperty(index = IndexType.BASIC)
    public String getFieldD4() {
        return null;
    }

    // Override and include:
    @Override
    @SpaceProperty
    public String getFieldD5() {
        return null;
    }

    // Override and downgrade index:
    @Override
    @SpaceProperty(index = IndexType.NONE)
    public String getFieldD6() {
        return null;
    }

    public String getFieldA() {
        return _fieldA;
    }

    public void setFieldA(String fieldA) {
        _fieldA = fieldA;
    }

    public String getFieldC() {
        return _fieldC;
    }

    public void setFieldC(String fieldC) {
        _fieldC = fieldC;
    }

    public String getFieldE() {
        return _fieldE;
    }

    public void setFieldE(String fieldE) {
        _fieldE = fieldE;
    }
}
