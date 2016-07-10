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

/**
 *
 */
package com.gigaspaces.internal.reflection;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceProperty;

@SpaceClass
@com.gigaspaces.api.InternalApi
public class ASMPropertiesTestObject {

    public static final int DEFAULT_Y = 666;

    private int nullValue;
    private Object o;
    private String privateString;
    private int x;
    private int y = DEFAULT_Y;

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    public Object getO() {
        return o;
    }

    public void setO(Object o) {
        this.o = o;
    }

    private String getPrivateString() {
        return privateString;
    }

    private void setPrivateString(String privateString) {
        this.privateString = privateString;
    }

    @SpaceProperty(nullValue = "-1")
    public int getNullValue() {
        return nullValue;
    }

    public void setNullValue(int nullValue) {
        this.nullValue = nullValue;
    }
}
