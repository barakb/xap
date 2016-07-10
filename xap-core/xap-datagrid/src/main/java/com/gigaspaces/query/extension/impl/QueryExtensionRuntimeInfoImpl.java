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

package com.gigaspaces.query.extension.impl;

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;

@com.gigaspaces.api.InternalApi
public class QueryExtensionRuntimeInfoImpl implements QueryExtensionRuntimeInfo {
    private final String nodeName;
    private final String workDir;

    public QueryExtensionRuntimeInfoImpl(String nodeName, String workDir) {
        this.nodeName = nodeName;
        this.workDir = workDir;
    }

    @Override
    public String getSpaceInstanceName() {
        return nodeName;
    }

    @Override
    public String getSpaceInstanceWorkDirectory() {
        return workDir;
    }
}
