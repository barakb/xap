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

package com.j_spaces.core;

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.Modifiers;

import net.jini.core.transaction.Transaction;

/**
 * a place holder for or the params used in update or write.
 *
 * @author asy ronen
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class UpdateOrWriteContext {
    public final IEntryPacket packet;
    public final long lease;
    public final long timeout;
    public final Transaction tx;
    public final SpaceContext sc;
    public int operationModifiers;
    public final boolean fromUpdateMultiple;

    public boolean isUpdateOperation;
    //GS-7339 used for workaround
    public final boolean fromWriteMultiple;
    //cache context - used by writeMultiple operation - so that all the entries use the same context
    private Context _cacheContext;
    //for new WriteMultiple - concentrating template
    private final ITemplateHolder _concentratingTemplate;
    private final int _ordinalInMultipleIdsContext;

    public UpdateOrWriteContext(IEntryPacket packet, long lease, long timeout, Transaction tx,
                                SpaceContext sc, int operationModifiers, boolean fromUpdateMultiple, boolean updateOperation,
                                boolean fromWriteMultiple) {
        this(packet, lease, timeout, tx,
                sc, operationModifiers, fromUpdateMultiple, updateOperation,
                fromWriteMultiple, null, -1);
    }

    public UpdateOrWriteContext(IEntryPacket packet, long lease, long timeout, Transaction tx,
                                SpaceContext sc, int operationModifiers, boolean fromUpdateMultiple, boolean updateOperation,
                                boolean fromWriteMultiple, ITemplateHolder concentratingTemplate, int ordinalInMultipleIdsContext) {
        this.packet = packet;
        this.lease = lease;
        this.timeout = timeout;
        this.tx = tx;
        this.sc = sc;
        this.operationModifiers = operationModifiers;
        this.fromUpdateMultiple = fromUpdateMultiple;
        isUpdateOperation = updateOperation;
        this.fromWriteMultiple = fromWriteMultiple;
        _concentratingTemplate = concentratingTemplate;
        _ordinalInMultipleIdsContext = ordinalInMultipleIdsContext;
    }

    public boolean isNoWriteLease() {
        return Modifiers.contains(operationModifiers, Modifiers.NO_WRITE_LEASE);
    }

    public boolean hasConcentratingTemplate() {
        return _concentratingTemplate != null;
    }

    public ITemplateHolder getConcentratingTemplate() {
        return _concentratingTemplate;
    }

    public int getOrdinalInMultipleIdsContext() {
        return _ordinalInMultipleIdsContext;
    }

    public Context getCacheContext() {
        return _cacheContext;
    }

    public void setCacheContext(Context context) {
        _cacheContext = context;
    }
}
