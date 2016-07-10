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

import com.gigaspaces.internal.utils.collections.IAddOnlySet;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.OperationID;

/**
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class BackupFailoverOperationIDFilter
        implements IDuplicateOperationFilter {

    private final IAddOnlySet<OperationID> _duplicateFilter;
    private final long _duplicateProtectionTimeInterval;
    private volatile boolean _backup;
    private volatile long _timeStampOfChangeFromBackupToPrimary;
    private volatile boolean _activeDuplicateProtection;

    public BackupFailoverOperationIDFilter(IAddOnlySet<OperationID> duplicateFilter, long duplicateProtectionTimeInterval) {
        _duplicateFilter = duplicateFilter;
        _duplicateProtectionTimeInterval = duplicateProtectionTimeInterval;
    }

    @Override
    public boolean contains(OperationID operationID) {
        if (operationID == null)
            return false;
        if (!_activeDuplicateProtection)
            return false;

        //Once time interval is elapsed we disable the protection
        long currentTime = SystemTime.timeMillis();
        if (currentTime - _timeStampOfChangeFromBackupToPrimary > _duplicateProtectionTimeInterval) {
            _activeDuplicateProtection = false;
            _duplicateFilter.clear();
            return false;
        }
        return _duplicateFilter.contains(operationID);
    }

    @Override
    public void add(OperationID operationID) {
        if (_backup)
            _duplicateFilter.add(operationID);
    }

    @Override
    public void onBecomeBackup() {
        _backup = true;
    }

    @Override
    public void onBecomePrimary() {
        if (_backup) {
            _timeStampOfChangeFromBackupToPrimary = SystemTime.timeMillis();
            _activeDuplicateProtection = true;
        } else {
            _backup = false;
            _activeDuplicateProtection = false;
        }
    }

    @Override
    public void clear() {
        _duplicateFilter.clear();
        _activeDuplicateProtection = false;
        _backup = false;
        _timeStampOfChangeFromBackupToPrimary = 0;
    }
}
