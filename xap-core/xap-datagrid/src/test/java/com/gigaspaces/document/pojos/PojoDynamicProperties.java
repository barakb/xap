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

package com.gigaspaces.document.pojos;

import com.gigaspaces.annotation.pojo.SpaceDynamicProperties;
import com.gigaspaces.internal.utils.ObjectUtils;

import java.util.Map;

@com.gigaspaces.api.InternalApi
public class PojoDynamicProperties {
    private String foo;
    private Map<String, Object> _dynamicProperties;

    public String getFoo() {
        return foo;
    }

    public void setFoo(String foo) {
        this.foo = foo;
    }

    @SpaceDynamicProperties
    public Map<String, Object> getDynamicProperties() {
        return _dynamicProperties;
    }

    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        this._dynamicProperties = dynamicProperties;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (!(obj instanceof PojoDynamicProperties))
            return false;

        PojoDynamicProperties other = (PojoDynamicProperties) obj;
        if (!ObjectUtils.equals(this.foo, other.foo))
            return false;
        if (!areEqual(this._dynamicProperties, other._dynamicProperties))
            return false;

        return true;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" +
                "foo=" + foo +
                ", dynamicProperties=" + _dynamicProperties +
                "]";
    }

    private static boolean areEqual(Map<String, Object> properties1, Map<String, Object> properties2) {
        if (properties1 == properties2)
            return true;
        if (properties1 == null || properties2 == null)
            return false;
        if (properties1.size() != properties2.size())
            return false;

        for (Map.Entry<String, Object> pair : properties1.entrySet()) {
            String key = pair.getKey();
            if (!properties2.containsKey(key))
                return false;
            Object value = properties2.get(key);
            if (!ObjectUtils.equals(pair.getValue(), value))
                return false;
        }

        return true;
    }
}
