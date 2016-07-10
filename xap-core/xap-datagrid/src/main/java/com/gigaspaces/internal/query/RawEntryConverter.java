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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class RawEntryConverter {

    private final IDirectSpaceProxy spaceProxy;
    private final ITemplatePacket queryPacket;
    private final boolean returnRawEntry;

    public RawEntryConverter(IDirectSpaceProxy spaceProxy, ITemplatePacket queryPacket, boolean returnRawEntry) {
        this.spaceProxy = spaceProxy;
        this.queryPacket = queryPacket;
        this.returnRawEntry = returnRawEntry;
    }

    public Object toObject(RawEntry rawEntry) {
        return spaceProxy.getTypeManager().convertQueryResult((IEntryPacket) rawEntry, queryPacket, returnRawEntry);
    }
}
