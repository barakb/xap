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

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceProperty;

@com.gigaspaces.api.InternalApi
public class PojoAnnotatedAndXmled {
    private String _someProperty1;
    private String _someProperty2;
    private String _someProperty3;

    @SpaceId(autoGenerate = false)
    public String getSomeProperty1() {
        return _someProperty1;
    }

    public void setSomeProperty1(String value) {
        _someProperty1 = value;
    }

    public String getSomeProperty2() {
        return _someProperty2;
    }

    public void setSomeProperty2(String value) {
        _someProperty2 = value;
    }

    @SpaceProperty
    public String getSomeProperty3() {
        return _someProperty3;
    }

    public void setSomeProperty3(String value) {
        _someProperty3 = value;
    }
}
