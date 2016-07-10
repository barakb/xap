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
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.UpdateOrWriteContext;
import com.j_spaces.core.XtnEntry;

import net.jini.space.JavaSpace;

/**
 * Context for by ids operations .
 *
 * @author yechiel
 * @since 9.6
 */
public abstract class MultipleIdsContext {
    private final int _numToAnswer;
    private final int _operationModifiers;
    private final ITemplateHolder _concentratingTemplate;
    private final long _timeOut;
    private final long _startTime;
    private final SpaceEngine _engine;
    private final XtnEntry _xtnEntry;

    private int _numAnswered;
    private final Object _lockObject;
    private volatile boolean _nonMainThreadUsed;
    private volatile boolean _answerReady;
    private final boolean[] _answered;


    public MultipleIdsContext(ITemplateHolder concentratingTemplate, int numToAnswer, int operationModifiers, long timeout,
                              SpaceEngine engine, XtnEntry xtnEntry) {
        _numToAnswer = numToAnswer;
        _concentratingTemplate = concentratingTemplate;
        _operationModifiers = operationModifiers;
        _timeOut = timeout;
        _answered = new boolean[numToAnswer];

        _lockObject = new Object();

        _engine = engine;
        _startTime = (_timeOut != JavaSpace.NO_WAIT) ? SystemTime.timeMillis() : 0;
        _xtnEntry = xtnEntry;
    }

    public boolean isNeedLocking() {
        return _nonMainThreadUsed;
    }

    public void setNonMainThreadUsed() {
        if (!_nonMainThreadUsed)
            _nonMainThreadUsed = true;
    }

    public Object getLockObject() {
        return _lockObject;
    }

    public ITemplateHolder getConcentratingTemplate() {
        return _concentratingTemplate;
    }

    public boolean isAnswerReady() {
        return _answerReady;
    }

    public long getTimeOut() {
        return _timeOut;
    }

    public int getOperationModifiers() {
        return _operationModifiers;
    }

    abstract Object[] getAnswer();


    public boolean setAnswer(Object res, int ordinal) {
        if (_nonMainThreadUsed) {
            synchronized (_lockObject) {
                return setAnswerImpl(res, ordinal);
            }
        } else {
            return setAnswerImpl(res, ordinal);
        }
    }


    private boolean setAnswerImpl(Object res, int ordinal) {
        //set the answer and indicate if all templates have reported completion
        if (!isAnswerSetForOrdinal(ordinal)) {
            setAnswerByOrdinal(res, ordinal);
            _answered[ordinal] = true;
            _numAnswered++;
            if (_numAnswered == _numToAnswer) {//set the answer in the answerholder of the concentrating template
                setAnswerInAnswerHolder(_concentratingTemplate.getAnswerHolder());
                _answerReady = true;    //also volatile used as a barrier
                return true;
            } else
                return false;
        } else
            return _numAnswered == _numToAnswer;
    }

    abstract void setAnswerByOrdinal(Object res, int ordinal);


    public boolean isAnswerSetForOrdinal(int ordinal) {
        if (_nonMainThreadUsed) {
            synchronized (_lockObject) {
                return isAnswerSetForOrdinal_impl(ordinal);
            }
        } else {
            return isAnswerSetForOrdinal_impl(ordinal);
        }
    }

    abstract void setAnswerInAnswerHolder(AnswerHolder ah);

    private boolean isAnswerSetForOrdinal_impl(int ordinal) {
        return _answered[ordinal];
    }


    public boolean isUpdateOrWriteMultiple() {
        return false;
    }

    public SpaceEngine getEngine() {
        return _engine;
    }

    public long getStartTime() {
        return _startTime;
    }


    boolean isExpired() {
        throw new UnsupportedOperationException();
    }

    /* if the exception is EntryNotInSpaceExecption shold we retry for this entry ? */
    public boolean shouldRetryUpdateOrWriteForEntry(Throwable exception, ITemplateHolder entryOpTemplate, UpdateOrWriteContext ctx, boolean spawnThread) {
        throw new UnsupportedOperationException();
    }

    public XtnEntry getXtnEntry() {
        return _xtnEntry;
    }

}
