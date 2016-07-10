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

package com.j_spaces.core.transaction;

import com.j_spaces.core.server.processor.BusPacket;
import com.j_spaces.core.server.processor.Processor;

import net.jini.core.transaction.server.ServerTransaction;

/**
 * check the state of a xtn in the tm using  callback Created by yechielf on 13/10/2015.
 */
@com.gigaspaces.api.InternalApi
public class CheckXtnStatusInTmBusPackect extends BusPacket<Processor> {
    private final ServerTransaction _tx;
    private final Object _notifyObj;
    private volatile boolean _notAbortedLiveTxn; //true means txn exists and not aborted yet
    private volatile boolean _hasAnswer;

    public CheckXtnStatusInTmBusPackect(ServerTransaction tx) {
        super(null /*operationID*/, null /*entryHolder*/, null, 0);
        _notifyObj = new Object();
        _tx = tx;
    }

    public ServerTransaction getTx() {
        return _tx;
    }

    public Object getNotifyObj() {
        return _notifyObj;
    }

    public boolean isNotAbortedLiveTxn() {
        return _notAbortedLiveTxn;
    }

    public boolean isHasAnswer() {
        return _hasAnswer;
    }

    public void setNotAbortedLiveTxn(boolean notAbortedLiveTxn) {
        this._notAbortedLiveTxn = notAbortedLiveTxn;
    }

    public void setHasAnswer(boolean hasAnswer) {
        this._hasAnswer = hasAnswer;
    }

    @Override
    public void execute(Processor processor) throws Exception {
        processor.handleCheckXtnStatusInTm(this);
    }


}
