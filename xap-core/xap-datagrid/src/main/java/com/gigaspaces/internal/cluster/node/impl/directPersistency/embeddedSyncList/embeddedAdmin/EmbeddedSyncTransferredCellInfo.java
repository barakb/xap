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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.embeddedAdmin;

/**
 * Embedded Sync- info regarding transferred records per segment
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedSyncTransferredCellInfo {
    public static final int TRANFERRED_CELL_CAPACITY_LIMIT = 100; //how many transferred each cell holds

    private final int _fromSeqNumber; //transferred from
    private final int _uptoSeqNumber; //transferred upto

    private int _numTransferred;

    EmbeddedSyncTransferredCellInfo(int fromSeqNumber, int uptoSeqNumber) {
        _fromSeqNumber = fromSeqNumber;
        _uptoSeqNumber = uptoSeqNumber;
    }

    EmbeddedSyncTransferredCellInfo(EmbeddedSyncTransferredCellInfo s1, EmbeddedSyncTransferredCellInfo s2) {
        if (s1.getFromSeqNumber() < s2.getFromSeqNumber()) {
            _fromSeqNumber = s1.getFromSeqNumber();
            _uptoSeqNumber = s2.getUptoSeqNumber();
        } else {
            _fromSeqNumber = s2.getFromSeqNumber();
            _uptoSeqNumber = s1.getUptoSeqNumber();
        }
        _numTransferred = s1.getNumTransferred() + s2.getNumTransferred();
    }

    public int getFromSeqNumber() {
        return _fromSeqNumber;
    }

    public int getUptoSeqNumber() {
        return _uptoSeqNumber;
    }

    public int getNumTransferred() {
        return _numTransferred;
    }

    public int addToTransferred() {
        return ++_numTransferred;
    }

    public boolean isCellFull() {
        return _numTransferred >= TRANFERRED_CELL_CAPACITY_LIMIT;
    }

    public boolean canJoin(EmbeddedSyncTransferredCellInfo other) {
        return (isCellFull() && other.isCellFull() && (other.getUptoSeqNumber() == getFromSeqNumber() - 1 || other.getFromSeqNumber() == getUptoSeqNumber() + 1));
    }

    StringBuffer displayInfo() {
        StringBuffer sb = new StringBuffer();
        sb.append(" from=").append(_fromSeqNumber).append(" to=").append(_uptoSeqNumber).append(" #transferred=").append(_numTransferred);
        return sb;
    }


}
