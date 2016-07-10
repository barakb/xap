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

import com.gigaspaces.internal.server.storage.IEntryData;

import java.util.ArrayList;
import java.util.List;

/**
 * extention of answer-holder contains prev and current entry snapshot + extended error info. NOTE-
 * used for single operation only
 *
 * @author yechielf
 */
@com.gigaspaces.api.InternalApi
public class ExtendedAnswerHolder extends AnswerHolder {

    //E.D. of the entry before the operation
    private IEntryData _previousEntryData;
    //E.D. of the entry after the operation
    private IEntryData _modifiedEntryData;
    //if an error occured- this is the entry which failed to be operated  upon
    private IEntryData _rejectedEntry;

    //batching- multipleEntries info
    List<IEntryData> _modifiedEntriesData;    //entries after change
    List _modifiedEntriesUids; //entries uids

    List<IEntryData> _rejectedEntries;
    List<Throwable> _rejectedCause;
    List _rejectedEntriesUids;

    //if detailed answer is required for change/changeMultiple-
    //for single change it contains a list of results (or null), for changeMultiple- list of lists of results which
    //corresponds to the _modifiedEntriesData list
    private List<Object> _changeResults;

    public ExtendedAnswerHolder() {
        super();
    }

    public IEntryData getPreviousEntryData() {
        return _previousEntryData;
    }

    public void setPreviousEntryData(IEntryData previousEntryData) {
        _previousEntryData = previousEntryData;
    }

    public IEntryData getModifiedEntryData() {
        return _modifiedEntryData;
    }

    public void setModifiedEntryData(IEntryData modifiedEntryData) {
        _modifiedEntryData = modifiedEntryData;
    }

    public IEntryData getRejectedEntry() {
        return _rejectedEntry;
    }

    public void setRejectedEntry(IEntryData ed) {
        _rejectedEntry = ed;
    }

    public void addModifiedEntriesData(IEntryData changed, Object uid, List<Object> changeResultsForEntry) {
        if (_modifiedEntriesData == null) {
            _modifiedEntriesData = new ArrayList<IEntryData>();
            _modifiedEntriesUids = new ArrayList();
        }
        _modifiedEntriesData.add(changed);
        _modifiedEntriesUids.add(uid);
        if (changeResultsForEntry != null && _changeResults == null) {
            _changeResults = new ArrayList<Object>();
            if (_modifiedEntriesData.size() > 1) {
                for (int i = 0; i < _modifiedEntriesData.size() - 1; i++)
                    _changeResults.add(null);
            }
        }
        if (_changeResults != null)
            _changeResults.add(changeResultsForEntry);
    }

    public void addRejectedEntriesInfo(Throwable t, IEntryData candidate, Object uid) {
        if (_rejectedEntries == null) {
            _rejectedEntries = new ArrayList<IEntryData>();
            _rejectedCause = new ArrayList<Throwable>();
            _rejectedEntriesUids = new ArrayList();
        }
        _rejectedEntries.add(candidate);
        _rejectedCause.add(t);
        _rejectedEntriesUids.add(uid);

    }

    public void resetRejectedEntriesInfo() {
        if (_rejectedEntries != null) {
            _rejectedEntries.clear();
            _rejectedCause.clear();
            _rejectedEntriesUids.clear();
        }
    }

    public boolean anyRejectedEntriesForMultipleOperation() {
        return (_rejectedEntries != null && !_rejectedEntries.isEmpty());
    }

    public boolean anyChangedEntriesForMultipleOperation() {
        return (_modifiedEntriesData != null && !_modifiedEntriesData.isEmpty());
    }

    public int getNumChangedEntriesForMultipleOperation() {
        return (_modifiedEntriesData != null ? _modifiedEntriesData.size() : 0);
    }

    public List<IEntryData> getModifiedEntriesData() {
        return _modifiedEntriesData;    //entries after change
    }

    public List getModifiedEntriesUids() {
        return _modifiedEntriesUids;//entries uids
    }

    public List<IEntryData> getRejectedEntries() {
        return _rejectedEntries;
    }

    public List<Throwable> getRejectedCause() {
        return _rejectedCause;
    }

    public List getRejectedEntriesUids() {
        return _rejectedEntriesUids;
    }

    public List<Object> getChangeResults() {
        return _changeResults;
    }

    public void setSingleChangeResults(List<Object> res) {
        _changeResults = res;
    }

}
