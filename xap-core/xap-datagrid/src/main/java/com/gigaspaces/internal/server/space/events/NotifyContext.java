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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds a counter that indicates how many notifications are still not returned from the client.
 *
 * When the number is getting to 0, it means that all the possible notifications where triggered and
 * that all of them already returned.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class NotifyContext {
    //starts from 1, this way the indication on finish all the templates search is over will set
    // the counter to 0.
    private AtomicInteger _notifyCount;
    private boolean _isFirstTrigger = true;

    private final IEntryHolder _notifyEntry;
    private final IEntryHolder _referenceEntry; // the updated entry in NOTIFY_UNMATCH cases or the old entry in NOTIFY_MATCHED/REMATCHED cases
    private final OperationID _operationId;
    private final NotifyActionType _notifyType;
    private boolean _guaranteedNotifications;
    private volatile boolean _finishedTemplateSearch;

    public NotifyContext(IEntryHolder notifyEntry, OperationID operationId, NotifyActionType notifyType) {
        this(notifyEntry, null, operationId, notifyType);
    }

    public NotifyContext(IEntryHolder notifyEntry, IEntryHolder referanceEntry,
                         OperationID operationId, NotifyActionType notifyType) {
        _notifyEntry = notifyEntry;
        _referenceEntry = referanceEntry;
        _operationId = operationId;
        _notifyType = notifyType;
    }

    public IEntryHolder getEntry() {
        return _notifyEntry;
    }

    public IEntryHolder getReferenceEntry() {
        return _referenceEntry;
    }

    public OperationID getOperationId() {
        return _operationId;
    }

    public NotifyActionType getNotifyType() {
        return _notifyType;
    }

    public boolean isGuaranteedNotifications() {
        return _guaranteedNotifications;
    }

    public void setGuaranteedNotifications(boolean guaranteedNotifications) {
        _guaranteedNotifications = guaranteedNotifications;
    }

    public boolean isFinishedTemplateSearch() {
        return _finishedTemplateSearch;
    }

    public void setFinishedTemplateSearch(boolean finishedTemplateSearch) {
        _finishedTemplateSearch = finishedTemplateSearch;
    }

    /**
     * @return true if the first increase
     */
    public boolean countInc(boolean needAfterAll) {
        if (_notifyCount != null)
            _notifyCount.incrementAndGet();
        else if (needAfterAll)
            _notifyCount = new AtomicInteger(2); // 2 is for: notification and after_all

        boolean first = _isFirstTrigger;
        _isFirstTrigger = false;
        return first;
    }

    /**
     * @return true if all the counter is 0 (no need to wait for more notifications to return)
     */
    public boolean countDec() {
        return _notifyCount == null ? false : _notifyCount.decrementAndGet() == 0;
    }
}
