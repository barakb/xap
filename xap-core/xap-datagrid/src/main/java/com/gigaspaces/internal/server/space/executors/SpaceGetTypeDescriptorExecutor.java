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

package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.actions.GetTypeDescriptorActionInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

/**
 * Gets type descriptor by type name.
 *
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceGetTypeDescriptorExecutor extends SpaceActionExecutor {
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        GetTypeDescriptorActionInfo actionInfo = (GetTypeDescriptorActionInfo) spaceRequestInfo;

        // Load type descriptors:
        actionInfo.resultTypeDescriptor = space.getEngine().getTypeManager().getTypeDesc(actionInfo.typeName);

        actionInfo.setResponse();
        return actionInfo;
    }

    @Override
    public boolean supportsBackup() {
        return true;
    }
}
