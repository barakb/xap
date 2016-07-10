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

package com.j_spaces.map;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceVersion;

/**
 * This class holds <code>key</code>, <code>value</code> and <code>Attribute</code>s that are
 * associated with the key. <code>key</code> is user-defined object that refers to the
 * <code>value</code>. <code>key</code>s are assumed to be unique.
 *
 * <b>For internal use only!</b>
 */
@com.gigaspaces.api.InternalApi
public class Envelope implements SpaceMapEntry {
    private Object key;
    private Object value;
    private String cacheId;
    private int version;

    public Envelope() {
        this(null, null, null);
    }

    public Envelope(Object key, Object value, String cacheID) {
        this.key = key;
        this.value = value;
        this.cacheId = cacheID;
    }

    @Override
    public String toString() {
        return "key: " + getKey() + " value: " + getValue();
    }

    @SpaceId
    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public void setKey(Object key) {
        this.key = key;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public Object setValue(Object value) {
        Object oldValue = getValue();
        this.value = value;
        return oldValue;
    }

    @Override
    public String getCacheId() {
        return cacheId;
    }

    public void setCacheId(String cacheId) {
        this.cacheId = cacheId;
    }

    @SpaceVersion
    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }
}
