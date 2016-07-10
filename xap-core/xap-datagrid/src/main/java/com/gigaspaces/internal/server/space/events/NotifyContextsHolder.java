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

package com.gigaspaces.internal.server.space.events;

import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.OperationID;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author yael
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class NotifyContextsHolder {
    protected final IEntryHolder _originalEntry;
    protected final IEntryHolder _newEntry;
    protected final OperationID _operationId;
    protected NotifyContext _notifyContext;
    private final boolean _isNotifyNewEntry;
    private NotifyActionType _notifyType;
    protected List<NotifyContext> _notifyContextsList;
    protected List<NotifyActionType> _notifyTypesList;


    public NotifyContextsHolder(IEntryHolder originalEntry, IEntryHolder newEntry, OperationID operationId, NotifyActionType notifyType) {
        _originalEntry = originalEntry;
        _newEntry = newEntry;
        _operationId = operationId;
        _isNotifyNewEntry = notifyType.equals(NotifyActionType.NOTIFY_UNMATCHED) || notifyType.equals(NotifyActionType.NOTIFY_TAKE) ? false : true;
        _notifyType = notifyType;
        _notifyContext = null;
        _notifyContextsList = Collections.EMPTY_LIST;
        _notifyTypesList = new LinkedList<NotifyActionType>();
        _notifyTypesList.add(notifyType);
    }

    public IEntryHolder getNotifyEntry() {
        return _isNotifyNewEntry ? _newEntry : _originalEntry;
    }

    public IEntryHolder getOriginalEntry() {
        return _originalEntry;
    }

    public IEntryHolder getNewEntry() {
        return _newEntry;
    }

    public NotifyContext getNotifyContext() {
        if (_notifyContext == null) {
            IEntryHolder notifyEntry = _newEntry;
            IEntryHolder referenceEntry = _originalEntry;
            if (!_isNotifyNewEntry) {
                notifyEntry = _originalEntry;
                referenceEntry = _newEntry;
            }
            _notifyContext = new NotifyContext(notifyEntry, referenceEntry, _operationId, _notifyType);
            if (_notifyContextsList == Collections.EMPTY_LIST)
                _notifyContextsList = new LinkedList<NotifyContext>();
            _notifyContextsList.add(_notifyContext);
        }
        return _notifyContext;
    }

    public List<NotifyContext> getNotifyContexts() {
        return _notifyContextsList;
    }

    public NotifyActionType getNotifyType() {
        return _notifyType;
    }

    public List<NotifyActionType> getNotifyTypes() {
        return _notifyTypesList;
    }
}
