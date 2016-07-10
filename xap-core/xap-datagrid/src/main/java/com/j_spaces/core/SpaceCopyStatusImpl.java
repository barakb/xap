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

import com.j_spaces.core.client.SpaceURL;

import net.jini.core.event.EventRegistration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@com.gigaspaces.api.InternalApi
public class SpaceCopyStatusImpl implements SpaceCopyStatus {
    private static final long serialVersionUID = 1L;

    public static final short RECOVERY_TYPE = 1;
    public static final short COPY_TYPE = 2;

    private short _operationType = COPY_TYPE;

    private int _totalCopyObj;
    private String _sourceMember;
    private final String _targetMember;
    private SpaceURL _sourceMemberUrl;
    private Exception _causeEx;

    private int _totalDummyObj;
    private AtomicInteger counter = new AtomicInteger();

    /**
     * key - className, value - count
     */
    private final HashMap<String, Integer> _writeEntries;
    private final HashMap<String, Integer> _notifyTempl;

    /**
     * Notify templates event registration list - used to cancel templates in case of a failure
     */
    private transient HashSet<EventRegistration> _notifyTemplRegistration;

    /**
     * key - className, value - UID
     */
    private final HashMap<String, String> _duplicateUID;

    /**
     * Constant for Notify NULL template
     */
    final static public String NULL_TEMPLATE = "NULL_NOTIFY_TEMPLATE";

    public SpaceCopyStatusImpl(short operationType, String targetMember) {
        _operationType = operationType;
        _targetMember = targetMember;

        _writeEntries = new HashMap<String, Integer>();
        _notifyTempl = new HashMap<String, Integer>();
        _duplicateUID = new HashMap<String, String>();
        _notifyTemplRegistration = new HashSet<EventRegistration>();
    }

    public int getTotalDummyObj() {
        return _totalDummyObj;
    }

    public void setTotalDummyObj(int totalDummyObj) {
        this._totalDummyObj = totalDummyObj;
    }

    public void incrementTotalDummyObjects() {
        _totalDummyObj++;
    }

    public int incrementCounter() {
        return counter.incrementAndGet();
    }

    public int getTotalCopyObj() {
        return _totalCopyObj;
    }

    public String getSourceMemberName() {
        return _sourceMember;
    }

    public String getTargetMemberName() {
        return _targetMember;
    }

    public SpaceURL getSourceMemberURL() {
        return _sourceMemberUrl;
    }

    public Exception getCauseException() {
        return _causeEx;
    }

    public void setCauseException(Exception causeEx) {
        _causeEx = causeEx;
    }

    public int getTotalBlockedEntriesByFilter() {
        return _totalDummyObj;
    }


    public Map<String, Integer> getTotalCopiedEntries() {
        return _writeEntries;
    }

    public Map<String, Integer> getTotalCopiedNotifyTemplates() {
        return _notifyTempl;
    }

    public Map<String, String> getTotalDuplicateEntries() {
        return _duplicateUID;
    }

    public void setOperationType(short operationType) {
        _operationType = operationType;
    }

    public HashMap<String, Integer> getWriteEntries() {
        return _writeEntries;
    }

    public HashMap<String, String> getDuplicateUID() {
        return _duplicateUID;
    }

    public HashSet<EventRegistration> getNotifyTemplRegistration() {
        return _notifyTemplRegistration;
    }

    public void setSourceMember(String sourceMember, SpaceURL sourceMemberUrl) {
        _sourceMember = sourceMember;
        _sourceMemberUrl = sourceMemberUrl;
    }

    public void addWriteEntry(String entryName) {
        increaseCount(entryName, _writeEntries);
    }

    public void setWriteCount(String className, int count) {
        _writeEntries.put(className, count);
        _totalCopyObj += count;
    }

    public void addNotifyTemplate(String templateName) {
        templateName = templateName == null ? NULL_TEMPLATE : templateName;
        increaseCount(templateName, _notifyTempl);
    }

    public void setNotifyTemplateCount(String className, int count) {
        _notifyTempl.put(className, count);
        _totalCopyObj += count;
    }

    public void addNotifyTemplate(String templateName, EventRegistration eventRegistration) {
        addNotifyTemplate(templateName);
        _notifyTemplRegistration.add(eventRegistration);
    }

    public void addDuplicateUID(String className, String UID) {
        _duplicateUID.put(UID, className);
    }

    private void increaseCount(String className, HashMap<String, Integer> mapType) {
        int total = 0;

        Integer count = mapType.get(className);
        if (count != null)
            total = count;

        mapType.put(className, ++total);

        _totalCopyObj++;
    }

    @Override
    public String toString() {
        if (_causeEx != null)
            return (_operationType == COPY_TYPE ? "Copy" : "Recovery") + " operation failed: " + _causeEx.toString();

        final StringBuilder sb = new StringBuilder();
        final String operation = _operationType == COPY_TYPE ? "Copied" : "Recovered";
        sb.append(operation + " from " + _sourceMember + " [" + _sourceMemberUrl + "] to " + _targetMember);
        sb.append(" [total-objects=").append(_totalCopyObj);
        appendEntriesDesc(sb, _writeEntries);
        appendNotifyTemplatesDesc(sb, _notifyTempl);
        appendDuplicatesDesc(sb, _duplicateUID);
        if (_totalDummyObj != 0)
            sb.append(",discarded-by-replication-input-filter=" + _totalDummyObj);
        sb.append("]");
        return sb.toString();
    }

    private void appendEntriesDesc(StringBuilder sb, Map<String, Integer> copiedEntries) {
        if (copiedEntries.size() != 0) {
            sb.append(",entries={");
            int total = 0;
            for (Map.Entry<String, Integer> entry : copiedEntries.entrySet()) {
                int count = entry.getValue();
                sb.append(entry.getKey() + "=" + count + ",");
                total += count;
            }
            sb.append("TOTAL=" + total + "}");
        }
    }

    private void appendNotifyTemplatesDesc(StringBuilder sb, Map<String, Integer> copiedNotifyTemplates) {
        if (copiedNotifyTemplates.size() != 0) {
            sb.append(",notify-templates={");
            int total = 0;
            for (Map.Entry<String, Integer> entry : copiedNotifyTemplates.entrySet()) {
                int count = entry.getValue();
                sb.append(entry.getKey() + "=" + count + ",");
                total += count;
            }
            sb.append("TOTAL=" + total + "}");
        }
    }

    private void appendDuplicatesDesc(StringBuilder sb, Map<String, String> duplicateEntries) {
        if (duplicateEntries.size() != 0) {
            sb.append(",duplicates=" + duplicateEntries.size());
            sb.append(" {");
            for (Map.Entry<String, String> entry : duplicateEntries.entrySet()) {
                String uid = entry.getKey();
                String className = entry.getValue();
                sb.append(uid + "/" + className + ",");
            }
            sb.setLength(sb.length() - 1);
            sb.append("}");
        }
    }
}
