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

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.annotation.pojo.SpaceProperty.IndexType;

@SpaceClass(inheritIndexes = false)
@com.gigaspaces.api.InternalApi
public class PojoIndexesInheritFalse extends PojoIndexes {
    private String _valueDefault2;
    private String _valueNone2;
    private String _valueBasic2;
    private String _valueExtended2;

    public String getValueDefault2() {
        return _valueDefault2;
    }

    public void setValueDefault2(String value) {
        _valueDefault2 = value;
    }

    @SpaceProperty(index = IndexType.NONE)
    public String getValueNone2() {
        return _valueNone2;
    }

    public void setValueNone2(String value) {
        _valueNone2 = value;
    }

    @SpaceProperty(index = IndexType.BASIC)
    public String getValueBasic2() {
        return _valueBasic2;
    }

    public void setValueBasic2(String value) {
        _valueBasic2 = value;
    }

    @SpaceProperty(index = IndexType.EXTENDED)
    public String getValueExtended2() {
        return _valueExtended2;
    }

    public void setValueExtended2(String value) {
        _valueExtended2 = value;
    }
}
