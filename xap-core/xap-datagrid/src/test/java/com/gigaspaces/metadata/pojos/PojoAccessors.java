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
public class PojoAccessors {
    private String _publicName;
    private String _protectedName;
    private String _privateName;
    private String _defaultName;

    public String getPublicName() {
        return _publicName;
    }

    public void setPublicName(String name) {
        this._publicName = name;
    }

    protected String getProtectedName() {
        return _protectedName;
    }

    protected void setProtectedName(String protectedName) {
        _protectedName = protectedName;
    }

    private String getPrivateName() {
        return _privateName;
    }

    private void setPrivateName(String privateName) {
        _privateName = privateName;
    }

    String getDefaultName() {
        return _defaultName;
    }

    void setDefaultName(String name) {
        _defaultName = name;
    }
}
