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

package com.gigaspaces.metadata.pojos;

@com.gigaspaces.api.InternalApi
public class PojoCommonSetters {
    private String _propertySimple;
    private String _propertyOriginal;
    private String _propertyFluent;
    private String _propertyOverload;

    public String getPropertySimple() {
        return _propertySimple;
    }

    public void setPropertySimple(String propertySimple) {
        this._propertySimple = propertySimple;
    }

    public String getPropertyOriginal() {
        return _propertyOriginal;
    }

    public String setPropertyOriginal(String propertyOriginal) {
        this._propertyOriginal = propertyOriginal;
        return this._propertyOriginal;
    }

    public String getPropertyFluent() {
        return _propertyFluent;
    }

    public PojoCommonSetters setPropertyFluent(String propertyFluent) {
        this._propertyFluent = propertyFluent;
        return this;
    }

    public String getPropertyOverload() {
        return _propertyOverload;
    }

    public void setPropertyOverload(String propertyOverload) {
        this._propertyOverload = propertyOverload;
    }

    public void setPropertyOverload(int propertyOverload) {
        this._propertyOverload = Integer.toString(propertyOverload);
    }

    private void foo() {
    }
}
