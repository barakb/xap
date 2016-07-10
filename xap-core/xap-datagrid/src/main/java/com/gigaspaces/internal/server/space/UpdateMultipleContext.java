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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.server.space.operations.WriteEntriesResult;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.AnswerPacket;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.client.OperationTimeoutException;
import com.j_spaces.core.client.UpdateModifiers;

/**
 * Context for update multiple operations. Accumulates  results and exceptions.
 *
 * @author yechiel
 * @since 9.6
 */

@com.gigaspaces.api.InternalApi
public class UpdateMultipleContext extends MultipleIdsContext {
    private final IEntryPacket[] _entriesToUpdate;
    private final long[] _leases;
    private final Object[] _answer;


    public UpdateMultipleContext(ITemplateHolder concentratingTemplate, IEntryPacket[] entries,
                                 long[] leases, int operationModifiers, long timeout, SpaceEngine engine, XtnEntry xtnEntry) {
        super(concentratingTemplate, entries.length, operationModifiers, timeout, engine, xtnEntry);
        _entriesToUpdate = entries;
        _leases = leases;
        _answer = new Object[entries.length];
    }

    @Override
    public Object[] getAnswer() {
        return _answer;
    }

    @Override
    void setAnswerByOrdinal(Object res, int ordinal) {
        if (res instanceof AnswerHolder) {
            AnswerHolder ah = (AnswerHolder) res;
            if (ah.getException() != null)
                _answer[ordinal] = ah.getException();
            else
                _answer[ordinal] = ah.getAnswerPacket().getWriteEntryResult();
//			   _answer[ordinal] =  super.isNewRouter()? ah.getAnswerPacket().getWriteEntryResult() : ah.getAnswerPacket().m_EntryPacket;
        } else
            _answer[ordinal] = res;
    }


    @Override
    void setAnswerInAnswerHolder(AnswerHolder ah) {
        Object[] updateMultipleResult = _answer;
        WriteEntriesResult result = new WriteEntriesResult(updateMultipleResult.length);
        for (int i = 0; i < updateMultipleResult.length; ++i) {
            if (updateMultipleResult[i] == null) //Update locked under transaction result
                result.setError(i, new OperationTimeoutException());
            else if (updateMultipleResult[i] instanceof Exception)
                result.setError(i, (Exception) updateMultipleResult[i]);
            else if (updateMultipleResult[i] instanceof WriteEntryResult)
                result.setResult(i, (WriteEntryResult) updateMultipleResult[i]);
            else if (updateMultipleResult[i] instanceof LeaseContext<?>) {
                LeaseContext<?> resultLease = (LeaseContext<?>) updateMultipleResult[i];
                IEntryPacket prevObject = (IEntryPacket) resultLease.getObject();
                WriteEntryResult writeResult = new WriteEntryResult(resultLease.getUID(), prevObject == null ? 1 : prevObject.getVersion() + 1, _leases[i]);
                result.setResult(i, writeResult);
            } else if (updateMultipleResult[i] instanceof IEntryPacket) {
                if (!UpdateModifiers.isNoReturnValue(super.getConcentratingTemplate().getOperationModifiers()))
                    result.setResult(i, new WriteEntryResult((IEntryPacket) updateMultipleResult[i]));
            }
        }

        for (int i = 0; i < result.getSize(); i++)
            if (!result.isError(i))
                result.getResults()[i].removeRedundantData(_entriesToUpdate[i].getTypeDescriptor(), super.getOperationModifiers());

        ah.setUpdateMultipleResult(result);
        ah.m_AnswerPacket = AnswerPacket.NullPacket;
    }


}
