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
 * An internal class for combining the matched and re-matched notifications context.
 *
 * @author yael
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class UpdateNotifyContextHolder extends NotifyContextsHolder {
    private NotifyContext _matchedNotifyContext;
    private NotifyContext _rematchedNotifyContext;


    public UpdateNotifyContextHolder(IEntryHolder originalEntry, IEntryHolder newEntry, OperationID operationId, boolean isMatched, boolean isRematched) {
        super(originalEntry, newEntry, operationId, NotifyActionType.NOTIFY_UPDATE);
        if (isMatched)
            _notifyTypesList.add(NotifyActionType.NOTIFY_MATCHED_UPDATE);
        if (isRematched)
            _notifyTypesList.add(NotifyActionType.NOTIFY_REMATCHED_UPDATE);
    }

    public NotifyContext getMatchedNotifyContext() {
        if (_matchedNotifyContext == null) {
            _matchedNotifyContext = new NotifyContext(_newEntry, _originalEntry, _operationId, NotifyActionType.NOTIFY_MATCHED_UPDATE);
            if (_notifyContextsList == Collections.EMPTY_LIST)
                _notifyContextsList = new LinkedList<NotifyContext>();
            _notifyContextsList.add(_matchedNotifyContext);
        }
        return _matchedNotifyContext;
    }


    public NotifyContext getRematchedNotifyContext() {
        if (_rematchedNotifyContext == null) {
            _rematchedNotifyContext = new NotifyContext(_newEntry, _originalEntry, _operationId, NotifyActionType.NOTIFY_REMATCHED_UPDATE);
            if (_notifyContextsList == Collections.EMPTY_LIST)
                _notifyContextsList = new LinkedList<NotifyContext>();
            _notifyContextsList.add(_rematchedNotifyContext);
        }
        return _rematchedNotifyContext;
    }

    @Override
    public List<NotifyActionType> getNotifyTypes() {
        return _notifyTypesList;
    }

}
