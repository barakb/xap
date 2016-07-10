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

package com.gigaspaces.serialization.pbs;

import java.util.HashMap;

@com.gigaspaces.api.InternalApi
public class DotnetGenericHashMap extends HashMap<Object, Object> implements IDoubleGenericType {
    private static final long serialVersionUID = -8334449503886134093L;

    private final String _keyGenericType;
    private final String _valueGenericType;

    public DotnetGenericHashMap(int capacity, float defaultLoadFactor,
                                String keyGenericType, String valueGenericType) {
        super(capacity, defaultLoadFactor);
        _keyGenericType = keyGenericType;
        _valueGenericType = valueGenericType;
    }

    public String getFirstGenericType() {
        return _keyGenericType;
    }

    public String getSecondGenericType() {
        return _valueGenericType;
    }

}
