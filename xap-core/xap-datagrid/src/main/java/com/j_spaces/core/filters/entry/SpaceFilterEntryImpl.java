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

package com.j_spaces.core.filters.entry;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.EntryImpl;

import java.rmi.MarshalledObject;

/**
 * Retrieves the data from the IEntryHolder, used by the space filters.
 *
 * @author Guy Korland
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceFilterEntryImpl extends EntryImpl implements ISpaceFilterEntry {
    private static final long serialVersionUID = -6011345301623313934L;

    public SpaceFilterEntryImpl(IEntryHolder entryHolder, ITypeDesc typeDesc) {
        super(entryHolder, typeDesc);
    }

    /*
     * Note that if IEntryHolder is TemplateHolder, returns a non-default value.
     * @see com.j_spaces.core.filters.entry.IFilterEntry#getHandback()
     */
    public MarshalledObject getHandback() {
        return super.getHandback();
    }

    /*
     * Note that if IEntryHolder is TemplateHolder, returns a non-default value.
     * @see com.j_spaces.core.filters.entry.IFilterEntry#getNotifyType()
     */
    public int getNotifyType() {
        return super.getNotifyType();
    }
}
