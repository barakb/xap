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

package com.gigaspaces.metadata.annotated;

import com.gigaspaces.annotation.pojo.SpaceId;

/**
 * Object to test that space id without autogenerate is implicitly indexed.
 *
 * @author Niv Ingberg
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class PojoIdIndexImplicit {
    private String _id;

    @SpaceId(autoGenerate = false)
    public String getId() {
        return _id;
    }

    public void setId(String id) {
        this._id = id;
    }
}
