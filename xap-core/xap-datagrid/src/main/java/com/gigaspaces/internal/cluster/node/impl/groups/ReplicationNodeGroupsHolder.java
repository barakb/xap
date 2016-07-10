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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfigBuilder;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeMode;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A components inside {@link ReplicationNode} that holds all the replication groups
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNodeGroupsHolder {
    private final Map<String, IReplicationSourceGroup> _replicationSourceGroups = new CopyOnUpdateMap<String, IReplicationSourceGroup>();
    private final Map<String, IReplicationTargetGroup> _replicationTargetGroups = new CopyOnUpdateMap<String, IReplicationTargetGroup>();

    private final Map<ReplicationNodeMode, Set<IReplicationSourceGroup>> _modeSourceGroups = new EnumMap<ReplicationNodeMode, Set<IReplicationSourceGroup>>(ReplicationNodeMode.class);
    private final Map<ReplicationNodeMode, Set<IReplicationTargetGroup>> _modeTargetGroups = new EnumMap<ReplicationNodeMode, Set<IReplicationTargetGroup>>(ReplicationNodeMode.class);

    //Do not generate garbage memory when iterating over the source/target groups
    private volatile IReplicationSourceGroup[] _replicationSourceGroupsArray = new IReplicationSourceGroup[0];
    private volatile IReplicationTargetGroup[] _replicationTargetGroupsArray = new IReplicationTargetGroup[0];

    public synchronized void addSourceGroup(IReplicationSourceGroup group, ReplicationNodeMode nodeMode) {
        _replicationSourceGroups.put(group.getGroupName(), group);
        Set<IReplicationSourceGroup> modeGroups = _modeSourceGroups.get(nodeMode);
        if (modeGroups == null) {
            modeGroups = new HashSet<IReplicationSourceGroup>();
            _modeSourceGroups.put(nodeMode, modeGroups);
        }
        modeGroups.add(group);

        reconstructSourceGroupsArray();
    }

    public synchronized void addTargetGroup(IReplicationTargetGroup group, ReplicationNodeMode nodeMode) {
        _replicationTargetGroups.put(group.getGroupName(), group);
        Set<IReplicationTargetGroup> modeGroups = _modeTargetGroups.get(nodeMode);
        if (modeGroups == null) {
            modeGroups = new HashSet<IReplicationTargetGroup>();
            _modeTargetGroups.put(nodeMode, modeGroups);
        }
        modeGroups.add(group);

        reconstructTargetGroupsArray();
    }

    public synchronized IReplicationTargetGroup addIfAbsentTargetGroup(
            IReplicationTargetGroup group, ReplicationNodeMode nodeMode) {
        IReplicationTargetGroup prevGroup = _replicationTargetGroups.get(group.getGroupName());
        if (prevGroup != null)
            return prevGroup;

        addTargetGroup(group, nodeMode);
        return group;
    }

    public synchronized void closeSourceGroups(ReplicationNodeMode nodeMode) {
        Set<IReplicationSourceGroup> groups = _modeSourceGroups.get(nodeMode);
        if (groups != null) {
            for (IReplicationSourceGroup sourceGroup : groups) {
                _replicationSourceGroups.remove(sourceGroup.getGroupName());
                sourceGroup.close();
            }
            groups.clear();
        }

        reconstructSourceGroupsArray();
    }

    public synchronized void closeTargetGroups(ReplicationNodeMode nodeMode) {
        Set<IReplicationTargetGroup> groups = _modeTargetGroups.get(nodeMode);
        if (groups != null) {
            for (IReplicationTargetGroup targetGroup : groups) {
                _replicationTargetGroups.remove(targetGroup.getGroupName());
                targetGroup.close();
            }
            groups.clear();
        }

        reconstructTargetGroupsArray();
    }

    private void reconstructSourceGroupsArray() {
        IReplicationSourceGroup[] groupsArray = new IReplicationSourceGroup[_replicationSourceGroups.size()];
        int index = 0;
        for (IReplicationSourceGroup sourceGroup : _replicationSourceGroups.values())
            groupsArray[index++] = sourceGroup;

        _replicationSourceGroupsArray = groupsArray;
    }

    private void reconstructTargetGroupsArray() {
        IReplicationTargetGroup[] groupsArray = new IReplicationTargetGroup[_replicationTargetGroups.size()];
        int index = 0;
        for (IReplicationTargetGroup targetGroup : _replicationTargetGroups.values())
            groupsArray[index++] = targetGroup;

        _replicationTargetGroupsArray = groupsArray;
    }

    public IReplicationSourceGroup getSourceGroup(String groupName) throws NoSuchReplicationGroupExistException {
        IReplicationSourceGroup sourceGroup = _replicationSourceGroups.get(groupName);
        if (sourceGroup != null)
            return sourceGroup;

        groupName = getBackwardMatchingGroupName(groupName, _replicationSourceGroups.keySet());
        sourceGroup = _replicationSourceGroups.get(groupName);
        if (sourceGroup != null)
            return sourceGroup;

        throw new NoSuchReplicationGroupExistException(groupName, "There is no replication source group under the name " + groupName + ". Registered groups = " + _replicationSourceGroups.keySet() + ".");
    }

    public IReplicationTargetGroup getTargetGroup(String groupName) throws NoSuchReplicationGroupExistException {
        IReplicationTargetGroup targetGroup = _replicationTargetGroups.get(groupName);
        if (targetGroup != null)
            return targetGroup;

        groupName = getBackwardMatchingGroupName(groupName, _replicationTargetGroups.keySet());
        targetGroup = _replicationTargetGroups.get(groupName);
        if (targetGroup != null)
            return targetGroup;

        throw new NoSuchReplicationGroupExistException(groupName, "There is no replication target group under the name " + groupName + ". Registered groups = " + _replicationTargetGroups.keySet() + ".");
    }

    private String getBackwardMatchingGroupName(String groupName, Set<String> groupNames) {
        if (groupName.startsWith(ReplicationNodeConfigBuilder.PRIMARY_BACKUP_SYNC) || groupName.startsWith(ReplicationNodeConfigBuilder.PRIMARY_BACKUP_ASYNC)) {
            boolean syncGroup = groupName.startsWith(ReplicationNodeConfigBuilder.PRIMARY_BACKUP_SYNC);
            String groupNameWithPartitionPrefix = null;
            for (String existingGroupName : groupNames) {
                if ((syncGroup && existingGroupName.startsWith(ReplicationNodeConfigBuilder.PRIMARY_BACKUP_SYNC)) ||
                        (!syncGroup && existingGroupName.startsWith(ReplicationNodeConfigBuilder.PRIMARY_BACKUP_ASYNC))) {
                    //Ambigious group, we cannot safely convert it, return the given name
                    if (groupNameWithPartitionPrefix != null)
                        return groupName;

                    groupNameWithPartitionPrefix = existingGroupName;
                }
            }
            return groupNameWithPartitionPrefix != null ? groupNameWithPartitionPrefix : groupName;
        }

        return groupName;
    }

    public IReplicationSourceGroup[] getSourceGroups() {
        return _replicationSourceGroupsArray;
    }

    public IReplicationTargetGroup[] getTargetGroups() {
        return _replicationTargetGroupsArray;
    }

    public synchronized void close() {
        for (IReplicationSourceGroup sourceGroup : getSourceGroups()) {
            sourceGroup.close();
        }
        for (IReplicationTargetGroup targetGroup : getTargetGroups()) {
            targetGroup.close();
        }
    }

    public String dumpState() {
        StringBuilder dump = new StringBuilder("---- Source Groups ----");
        IReplicationSourceGroup[] sourceGroups = getSourceGroups();
        if (sourceGroups.length == 0) {
            dump.append(StringUtils.NEW_LINE);
            dump.append("NONE");
        }
        for (IReplicationSourceGroup sourceGroup : sourceGroups) {
            dump.append(StringUtils.NEW_LINE);
            dump.append(sourceGroup.dumpState());
        }
        dump.append(StringUtils.NEW_LINE);
        dump.append("---- Target Groups ----");
        IReplicationTargetGroup[] targetGroups = getTargetGroups();
        if (targetGroups.length == 0) {
            dump.append(StringUtils.NEW_LINE);
            dump.append("NONE");
        }
        for (IReplicationTargetGroup targetGroup : targetGroups) {
            dump.append(StringUtils.NEW_LINE);
            dump.append(targetGroup.dumpState());
        }
        return dump.toString();
    }

}
