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

package com.gigaspaces.internal.client.spaceproxy.actioninfo;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import net.jini.core.transaction.Transaction;

/**
 * @author Niv Ingberg
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeAsyncProxyActionInfo extends ReadTakeProxyActionInfo {
    public final AsyncFutureListener listener;

    public ReadTakeAsyncProxyActionInfo(ISpaceProxy spaceProxy, Object template, Transaction txn, long timeout,
                                        int modifiers, boolean isTake, AsyncFutureListener<?> listener) {
        super(spaceProxy, template, txn, timeout, modifiers, false, isTake);
        this.listener = listener;

        if (isSqlQuery)
            this.queryPacket = spaceProxy.getDirectProxy().getQueryManager().getSQLTemplate((SQLQueryTemplatePacket) this.queryPacket, txn);
    }

    @Override
    public boolean isAsync() {
        return true;
    }
}
