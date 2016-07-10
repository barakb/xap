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

package com.gigaspaces.cluster.activeelection.core;

import com.gigaspaces.internal.utils.StringUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;


@com.gigaspaces.api.InternalApi
public class ServiceReplicationStatus
        implements Comparable<ServiceReplicationStatus> {
    public static final long CONSISTENT_REPLICATION_PRIORITY = 0;
    public static final long LOWEST_REPLICATION_PRIORITY = Long.MAX_VALUE;

    public static final long INACTIVE_FACTOR = 1000;
    public static final long INCONSISTENT_WITH_MIRROR_FACTOR = 1000 * 1000 * 1000;
    public static final long INCONSISTENT_WITH_SPACE_FACTOR = 1000 * 1000;

    public static ServiceReplicationStatus UNKNOWN = new ServiceReplicationStatus(LOWEST_REPLICATION_PRIORITY,
            "Unknown replication status");
    public static ServiceReplicationStatus UNREACHABLE_TARGET = new ServiceReplicationStatus(LOWEST_REPLICATION_PRIORITY,
            "Unreachable target");
    public long _priority;
    public final LinkedList<String> _description = new LinkedList<String>();

    private Set<String> _replicationTargetsNames;
    private long _processId;

    public ServiceReplicationStatus() {
        _priority = ServiceReplicationStatus.CONSISTENT_REPLICATION_PRIORITY;
    }

    public ServiceReplicationStatus(long priority,
                                    String serviceReplicationStatus) {
        super();
        _priority = priority;
        _description.add(serviceReplicationStatus);
    }

    public void reducePriority(long reductionFactor, String description) {
        _description.addLast(description);

        if (Integer.MAX_VALUE - reductionFactor < _priority)
            _priority = Integer.MAX_VALUE;
        else
            _priority = _priority + reductionFactor;

    }

    public long getPriority() {
        return _priority;
    }

    public String getDescription() {
        if (_description.isEmpty())
            return "Consistent replication state";
        return _description.toString();
    }

    @Override
    public int compareTo(ServiceReplicationStatus o) {
        if (_priority == o.getPriority())
            return 0;

        return _priority > o.getPriority() ? 1 : -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("replicationStatus=[inconsistency rank=" + _priority + " (closer to 0 is more consistent)]:" + StringUtils.NEW_LINE);

        if (_description.isEmpty()) {
            sb.append(" - Consistent replication state");
        } else {
            for (int i = 0; i < _description.size(); i++) {
                String description = _description.get(i);
                sb.append(" - " + description);

                if (i < _description.size() - 1)
                    sb.append(StringUtils.NEW_LINE);
            }
        }
        return sb.toString();
    }

    public boolean containsReplicationTarget(String targetMemberName) {
        return _replicationTargetsNames != null && _replicationTargetsNames.contains(targetMemberName);
    }

    public void addReplicationTargetName(String targetMemberName) {
        if (_replicationTargetsNames == null)
            _replicationTargetsNames = new HashSet<String>();
        _replicationTargetsNames.add(targetMemberName);
    }

    public void setProcessId(long processId) {
        this._processId = processId;
    }

    public long getProcessId() {
        return _processId;
    }

}