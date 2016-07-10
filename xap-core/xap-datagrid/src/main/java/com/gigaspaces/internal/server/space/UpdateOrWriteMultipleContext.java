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

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.AnswerPacket;
import com.j_spaces.core.LeaseManager;
import com.j_spaces.core.UpdateOrWriteContext;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.server.processor.UpdateOrWriteBusPacket;

import net.jini.space.JavaSpace;

/**
 * Context for updateOrW  multiple operations. Accumulates  results and exceptions.
 *
 * @author yechiel
 * @since 9.6
 */

@com.gigaspaces.api.InternalApi
public class UpdateOrWriteMultipleContext extends UpdateMultipleContext {

    public UpdateOrWriteMultipleContext(ITemplateHolder concentratingTemplate, IEntryPacket[] entries,
                                        long[] leases, int operationModifiers, long timeout, SpaceEngine engine, XtnEntry xtnEntry) {
        super(concentratingTemplate, entries,
                leases, operationModifiers, timeout, engine, xtnEntry);
    }

    @Override
    public boolean isUpdateOrWriteMultiple() {
        return true;
    }

    @Override
    boolean isExpired() {
        if (super.getTimeOut() == JavaSpace.NO_WAIT)
            return false;
        final long current = SystemTime.timeMillis();
        long expTime = LeaseManager.toAbsoluteTime(super.getTimeOut(), super.getStartTime());
        return expTime < current;
    }

    @Override
    /* if the exception is EntryNotInSpaceExecption shold we retry for this entry ? */
    public boolean shouldRetryUpdateOrWriteForEntry(Throwable exception, ITemplateHolder entryOpTemplate, UpdateOrWriteContext ctx, boolean spawnThread) {

        if ((exception instanceof EntryNotInSpaceException) && !isExpired()) {
            if (spawnThread) {
                super.setNonMainThreadUsed();
                UpdateOrWriteBusPacket packet = new UpdateOrWriteBusPacket(ctx.packet.getOperationID(), super.getConcentratingTemplate().getResponseContext(), ctx);
                super.getEngine().getProcessorWG().enqueueBlocked(packet);
            }
            return true;
        } else
            return false;

    }


    @Override
    void setAnswerByOrdinal(Object res, int ordinal) {
        Object[] answer = super.getAnswer();
        if (res instanceof AnswerHolder) {
            AnswerHolder ah = (AnswerHolder) res;
            AnswerPacket answerPacket = ah.m_AnswerPacket;
            if (ah.getException() != null)
                answer[ordinal] = ah.getException();
            else if (answerPacket.m_leaseProxy != null)
                answer[ordinal] = answerPacket.m_leaseProxy;
            else if (answerPacket.getWriteEntryResult() != null)
                answer[ordinal] = answerPacket.getWriteEntryResult();
            else
                answer[ordinal] = answerPacket.m_EntryPacket;
        } else
            answer[ordinal] = res;
    }
}
