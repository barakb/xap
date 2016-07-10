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

package com.gigaspaces.metadata.gsxml;


@com.gigaspaces.api.InternalApi
public class PojoBasicStorageTypeXml {
    private Object _id;
    private Object _routing;
    private String _explicitDefaultStorageTypeOnPrimitive;
    private Boolean _explicitObjectStorageTypeOnPrimitive;
    private int _noStorageTypePrimitive;
    private Object _explicitDefaultStorageTypeObject;
    private Object _objectStorageTypeObject;
    private Object _binaryStorageTypeObject;
    private Object _compressedStorageTypeObject;

    public PojoBasicStorageTypeXml() {
    }

    public Object get_id() {
        return _id;
    }

    public void set_id(Object _id) {
        this._id = _id;
    }

    public Object get_routing() {
        return _routing;
    }

    public void set_routing(Object _routing) {
        this._routing = _routing;
    }

    public String get_explicitDefaultStorageTypeOnPrimitive() {
        return _explicitDefaultStorageTypeOnPrimitive;
    }

    public void set_explicitDefaultStorageTypeOnPrimitive(String _explicitDefaultStorageTypeOnPrimitive) {
        this._explicitDefaultStorageTypeOnPrimitive = _explicitDefaultStorageTypeOnPrimitive;
    }

    public Boolean get_explicitObjectStorageTypeOnPrimitive() {
        return _explicitObjectStorageTypeOnPrimitive;
    }

    public void set_explicitObjectStorageTypeOnPrimitive(Boolean _explicitObjectStorageTypeOnPrimitive) {
        this._explicitObjectStorageTypeOnPrimitive = _explicitObjectStorageTypeOnPrimitive;
    }

    public int get_noStorageTypePrimitive() {
        return _noStorageTypePrimitive;
    }

    public void set_noStorageTypePrimitive(int _noStorageTypePrimitive) {
        this._noStorageTypePrimitive = _noStorageTypePrimitive;
    }

    public Object get_explicitDefaultStorageTypeObject() {
        return _explicitDefaultStorageTypeObject;
    }

    public void set_explicitDefaultStorageTypeObject(Object _explicitDefaultStorageTypeObject) {
        this._explicitDefaultStorageTypeObject = _explicitDefaultStorageTypeObject;
    }

    public Object get_objectStorageTypeObject() {
        return _objectStorageTypeObject;
    }

    public void set_objectStorageTypeObject(Object _objectStorageTypeObject) {
        this._objectStorageTypeObject = _objectStorageTypeObject;
    }

    public Object get_binaryStorageTypeObject() {
        return _binaryStorageTypeObject;
    }

    public void set_binaryStorageTypeObject(Object _binaryStorageTypeObject) {
        this._binaryStorageTypeObject = _binaryStorageTypeObject;
    }

    public Object get_compressedStorageTypeObject() {
        return _compressedStorageTypeObject;
    }

    public void set_compressedStorageTypeObject(Object _compressedStorageTypeObject) {
        this._compressedStorageTypeObject = _compressedStorageTypeObject;
    }
}
