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

package com.gigaspaces.lrmi.classloading;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA. User: assafr Date: 14/05/2008 Time: 21:43:21 To change this template
 * use File | Settings | File Templates.
 */
@com.gigaspaces.api.InternalApi
public class ClassProviderRequest implements Serializable {

    private static final long serialVersionUID = 1L;
    //TODO make externalizable for future backwards
    private final Long _classLoaderId;

    public ClassProviderRequest(Long classLoaderId) {
        _classLoaderId = classLoaderId;
    }

    //TODO remove this, only created for backward compatible with current lrmi protocol that needs to be
    //changed as well
    public ClassProviderRequest() {
        _classLoaderId = -1L;
    }

    public Long getClassLoaderId() {
        return _classLoaderId;
    }
}
