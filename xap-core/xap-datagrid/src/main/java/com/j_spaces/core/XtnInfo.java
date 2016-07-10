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
/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

import com.gigaspaces.internal.utils.concurrent.ReentrantSimpleLock;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.client.LocalTransactionManager;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.Set;

@com.gigaspaces.api.InternalApi
public class XtnInfo extends ReentrantSimpleLock {
    public final ServerTransaction m_Transaction;
    private volatile XtnStatus _status;                        // of type XtnStatus
    public final boolean m_Readonly;
    public boolean m_Active;
    public boolean m_AlreadyPrepared;            // set to true on prepare
    public long m_CommitRollbackTimeStamp;
    public boolean m_SingleParticipant;
    public boolean m_AnyUpdates;                    // true if any update performed
    public final long m_startTime;
    private boolean _isFromReplication;
    private XtnGatewayInfo _gatewayInfo = null;

    //is this xtn currently used by a thread .
    private int _usage;
    private long _lastUsageTime;
    //was this xtn operated upon (i.e. successful operation with it) ?
    private boolean _operatedUpon;
    //can we clean if this xtn is unused & global ?
    private boolean _onlyEmbeddedJoins;
    private boolean _disableUnusedXtnCleanupInServer;

    /**
     * Constructs a new Xtn info.
     */
    public XtnInfo(ServerTransaction xtn) {
        m_Transaction = xtn;
        setStatus(XtnStatus.UNINITIALIZED);
        m_Readonly = true;
        m_Active = true;
        m_startTime = SystemTime.timeMillis();
        _usage = 1;
        _lastUsageTime = m_startTime;
        _onlyEmbeddedJoins = true;
    }

    public ServerTransaction getServerTransaction() {
        return m_Transaction;
    }

    public long getStartTime() {
        return m_startTime;
    }

    /**
     * @return the isFromReplication
     */
    public boolean isFromReplication() {
        return _isFromReplication;
    }

    /**
     * @param isFromReplication the isFromReplication to set
     */
    public void setFromReplication(boolean isFromReplication) {
        _isFromReplication = isFromReplication;
    }

    /**
     * @param m_Status the m_Status to set
     */
    public void setStatus(XtnStatus m_Status) {
        this._status = m_Status;
    }

    /**
     * @return the m_Status
     */
    public XtnStatus getStatus() {
        return _status;
    }

    public boolean isUsed() {
        return _usage > 0;
    }

    //add to  used, if succeded return true
    public synchronized boolean addUsedIfPossible() {
        if (_status == XtnStatus.UNUSED)
            return false;
        _usage++;
        return true;

    }

    public void decrementUsed() {
        decrementUsed(false /*disableUnusedXtnCleanupInServer*/);
    }

    public synchronized void decrementUsed(boolean disableUnusedXtnCleanupInServer) {
        _lastUsageTime = SystemTime.timeMillis();
        if (_usage > 0)
            _usage--;
        if (disableUnusedXtnCleanupInServer)
            _disableUnusedXtnCleanupInServer = true;
    }

    //set status to unused, if succeded return true 
    public boolean setUnUsedIfPossible(int unusedCleanTime, boolean includeGlobalXtns) {
        if (!isCandidateForUnusedXtnRemoval(includeGlobalXtns))
            return false;
        if (_status == XtnStatus.UNUSED)
            return true;

        synchronized (this) {
            if (isOperatedUpon() || isUsed() || _status != XtnStatus.BEGUN || !isOnlyEmbeddedJoins() || _disableUnusedXtnCleanupInServer)
                return false;
            if (_lastUsageTime + unusedCleanTime >= SystemTime.timeMillis())
                return false;
            _status = XtnStatus.UNUSED;
            return true;

        }
    }

    public boolean isOperatedUpon() {
        return _operatedUpon;
    }

    public void setOperatedUpon() {
        if (isOperatedUpon())
            return;
        synchronized (this) {//we use synchronize instead of volatile cause we do it only once, no need for multiple touches of volatile
            _operatedUpon = true;
        }
    }

    public long getLastUsageTime() {
        return _lastUsageTime;
    }

    private boolean isCandidateForUnusedXtnRemoval(boolean includeGlobalXtns) {
        return includeGlobalXtns || (m_Transaction.mgr instanceof LocalTransactionManager);
    }

    public synchronized boolean isOnlyEmbeddedJoins() {
        return _onlyEmbeddedJoins;
    }

    public synchronized boolean setOnlyEmbeddedJoins(boolean val) {
        boolean old = _onlyEmbeddedJoins;
        _onlyEmbeddedJoins = val;
        return old;
    }

    /**
     * @return true if the entry was created by a gateway operation.
     */
    public boolean isFromGateway() {
        return _gatewayInfo != null;
    }

    public void setFromGateway() {
        if (_gatewayInfo == null)
            _gatewayInfo = XtnGatewayInfo.EMPTY_GATEWAY_INDICATOR;
    }

    public void setGatewayOverrideVersion(String uid) {
        if (_gatewayInfo == null || _gatewayInfo == XtnGatewayInfo.EMPTY_GATEWAY_INDICATOR) {
            _gatewayInfo = new XtnGatewayInfo();
        }
        _gatewayInfo.setOverrideVersion(uid);
    }

    public Set<String> getOverrideVersionUids() {
        if (_gatewayInfo != null)
            return _gatewayInfo.getOverrideVersionUids();
        return null;
    }
}