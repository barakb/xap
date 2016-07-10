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

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.AbstractIdsQueryPacket;
import com.j_spaces.core.AnswerPacket;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by boris on 12/30/2014. Contains the context for each readByIds (also used in clearByIds
 * - AKA clear(IdsQuery query)). Since 10.1.0
 */
@com.gigaspaces.api.InternalApi
public class ReadByIdsContext implements Serializable {

    private static final long serialVersionUID = -5416359726050399719L;

    private ITemplatePacket _template;
    private boolean _accumulate;
    private IEntryPacket[] _results;
    private Map<Object, Exception> _failures;
    private int _successCount = 0;


    public ReadByIdsContext(ITemplatePacket template, boolean accumulate) {
        this._template = template;
        this._accumulate = accumulate;
        this._failures = new HashMap<Object, Exception>();
    }

    /**
     * @return true if the context should accumulate results, false otherwise
     */
    public boolean accumulate() {
        return _accumulate;
    }

    /**
     * if the operation not requires to accumulate set null, otherwise initialize the results for
     * the readByIds operation
     */
    public void initResultsEntryPackets() {
        _results = !accumulate() ? null : new IEntryPacket[((AbstractIdsQueryPacket) _template).getIds().length];
    }

    /**
     * Apply the result - store the result (readByIds) or count the cleared objects (clearByIds)
     *
     * @param answerPacket the answer which were returned from the readById request
     * @param resultIndex  the index of the result
     */
    public void applyResult(AnswerPacket answerPacket, int resultIndex) {
        if (accumulate()) {
            _results[resultIndex] = answerPacket.m_EntryPacket;
        } else {
            if (answerPacket.m_EntryPacket != null)
                _successCount++;
        }
    }

    public int getSuccessCount() {
        return _successCount;
    }

    public IEntryPacket[] getResults() {
        return _results;
    }

    public ITemplatePacket getTemplate() {
        return _template;
    }

    public Map<Object, Exception> getFailures() {
        return _failures;
    }

    public void addFailure(Object id, Exception e) {
        _failures.put(id, e);
    }
}
