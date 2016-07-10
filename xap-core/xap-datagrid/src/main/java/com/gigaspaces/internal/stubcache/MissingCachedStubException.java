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

package com.gigaspaces.internal.stubcache;

import com.gigaspaces.exception.lrmi.LRMIUnhandledException;

/**
 * Thrown to indicate that the requested stub does not exists in the stub cache
 *
 * @author eitany
 * @since 7.5
 */
@com.gigaspaces.api.InternalApi
public class MissingCachedStubException
        extends LRMIUnhandledException {
    /** */
    private static final long serialVersionUID = 1L;

    private final StubId _stubId;

    public MissingCachedStubException(StubId stubId) {
        super(Stage.DESERIALIZATION, "No stub with id " + stubId + " found in stub cache");
        this._stubId = stubId;
    }

    public StubId getStubId() {
        return _stubId;
    }

}
