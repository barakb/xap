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

package com.j_spaces.core.client.sql;

import com.gigaspaces.internal.client.spaceproxy.actioninfo.CountClearProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeMultipleProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeProxyActionInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;

import java.rmi.RemoteException;

/**
 * Interface for query handling
 *
 * @author anna
 * @since 6.5
 */
public interface IQueryManager {
    ITemplatePacket getSQLTemplate(SQLQueryTemplatePacket template, Transaction txn);

    int countClear(CountClearProxyActionInfo actionInfo);

    IEntryPacket readTake(ReadTakeProxyActionInfo actionInfo) throws RemoteException, UnusableEntryException;

    IEntryPacket[] readTakeMultiple(ReadTakeMultipleProxyActionInfo actionInfo);

    void clean();
}
