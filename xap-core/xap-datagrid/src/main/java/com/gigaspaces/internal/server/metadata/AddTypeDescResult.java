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

package com.gigaspaces.internal.server.metadata;


/**
 * A tuple for holding an {@link IServerTypeDesc} and {@link AddTypeDescResultType} as a result of a
 * {@link com.gigaspaces.internal.server.space.metadata.SpaceTypeManager#addTypeDesc} invocation.
 *
 * @author Idan Moyal
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class AddTypeDescResult {
    private final IServerTypeDesc _serverTypeDesc;
    private final AddTypeDescResultType _addTypeDescAction;

    public AddTypeDescResult(
            IServerTypeDesc serverTypeDesc,
            AddTypeDescResultType registrationAction) {
        this._serverTypeDesc = serverTypeDesc;
        this._addTypeDescAction = registrationAction;
    }

    public AddTypeDescResultType getResultType() {
        return _addTypeDescAction;
    }

    public IServerTypeDesc getServerTypeDesc() {
        return _serverTypeDesc;
    }
}
