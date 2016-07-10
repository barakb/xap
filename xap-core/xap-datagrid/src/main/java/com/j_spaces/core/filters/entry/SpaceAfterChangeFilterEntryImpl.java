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
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.sync.change.ChangeOperation;
import com.gigaspaces.sync.change.DataSyncChangeSet;

import java.util.Collection;

/**
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class SpaceAfterChangeFilterEntryImpl
        extends SpaceUpdateFilterEntryImpl
        implements ISpaceFilterEntry, DataSyncChangeSet {

    private static final long serialVersionUID = 1L;

    private final Collection _mutators;
    private final ITemplateHolder _templateHolder;

    public SpaceAfterChangeFilterEntryImpl(ITemplateHolder templateHolder, IEntryPacket entryPacket,
                                           ITypeDesc typeDesc, Collection mutators) {
        super(entryPacket, typeDesc);
        _templateHolder = templateHolder;
        _mutators = mutators;
    }


    @Override
    public Collection<ChangeOperation> getOperations() {
        return _mutators;
    }

    @Override
    public Object getId() {
        return _templateHolder.getEntryId();
    }

    @Override
    public int getVersion() {
        return _templateHolder.getEntryData().getVersion();
    }

}
