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

import com.gigaspaces.annotation.pojo.SpaceStorageType;
import com.gigaspaces.metadata.StorageType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@com.gigaspaces.api.InternalApi
public class PojoOnlyStorageTypeAnnotations {
    Integer _integer;
    String _string;
    Boolean _boolean;
    Long _long;

    Object _objectStorageTypeObject;
    List<String> _binaryStorageTypeList;
    Map<String, Object> _compressedStorageTypeMap;
    ArrayList<Object> _defaultStorageTypeArrayList;

    @SpaceStorageType(storageType = StorageType.OBJECT)
    public Integer get_integer() {
        return _integer;
    }

    public void set_integer(Integer _integer) {
        this._integer = _integer;
    }

    @SpaceStorageType(storageType = StorageType.DEFAULT)
    public String get_string() {
        return _string;
    }

    public void set_string(String _string) {
        this._string = _string;
    }

    @SpaceStorageType(storageType = StorageType.OBJECT)
    public Boolean get_boolean() {
        return _boolean;
    }

    public void set_boolean(Boolean _boolean) {
        this._boolean = _boolean;
    }

    public Long get_long() {
        return _long;
    }

    public void set_long(Long _long) {
        this._long = _long;
    }

    @SpaceStorageType(storageType = StorageType.OBJECT)
    public Object get_objectStorageTypeObject() {
        return _objectStorageTypeObject;
    }

    public void set_objectStorageTypeObject(Object _objectStorageTypeObject) {
        this._objectStorageTypeObject = _objectStorageTypeObject;
    }

    @SpaceStorageType(storageType = StorageType.BINARY)
    public List<String> get_binaryStorageTypeList() {
        return _binaryStorageTypeList;
    }

    public void set_binaryStorageTypeList(List<String> _binaryStorageTypeList) {
        this._binaryStorageTypeList = _binaryStorageTypeList;
    }

    @SpaceStorageType(storageType = StorageType.COMPRESSED)
    public Map<String, Object> get_compressedStorageTypeMap() {
        return _compressedStorageTypeMap;
    }

    public void set_compressedStorageTypeMap(Map<String, Object> _compressedStorageTypeMap) {
        this._compressedStorageTypeMap = _compressedStorageTypeMap;
    }

    @SpaceStorageType(storageType = StorageType.DEFAULT)
    public ArrayList<Object> get_defaultStorageTypeArrayList() {
        return _defaultStorageTypeArrayList;
    }

    public void set_defaultStorageTypeArrayList(ArrayList<Object> _defaultStorageTypeArrayList) {
        this._defaultStorageTypeArrayList = _defaultStorageTypeArrayList;
    }


}
