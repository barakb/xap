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
public class PojoStorageTypeInheritAndOverride extends PojoPropertiesStorageType {

    @Override
    public Object getId() {
        // TODO Auto-generated method stub
        return super.getId();
    }

    @Override
    @SpaceStorageType(storageType = StorageType.OBJECT)
    public long getSomeLong() {
        // TODO Auto-generated method stub
        return super.getSomeLong();
    }

    @Override
    public ArrayList<Object> get_defaultStorageTypeArrayList() {
        // TODO Auto-generated method stub
        return super.get_defaultStorageTypeArrayList();
    }

    @Override
    @SpaceStorageType(storageType = StorageType.BINARY)
    public List<String> get_binaryStorageTypeList() {
        // TODO Auto-generated method stub
        return super.get_binaryStorageTypeList();
    }

    @Override
    @SpaceStorageType(storageType = StorageType.DEFAULT)
    public Map<String, Object> get_compressedStorageTypeMap() {
        // TODO Auto-generated method stub
        return super.get_compressedStorageTypeMap();
    }

    @Override
    @SpaceStorageType(storageType = StorageType.DEFAULT)
    public Object getNoneIndexObject() {
        // TODO Auto-generated method stub
        return super.getNoneIndexObject();
    }

    @Override
    public long getLeasExpiration() {
        // TODO Auto-generated method stub
        return super.getLeasExpiration();
    }

}
