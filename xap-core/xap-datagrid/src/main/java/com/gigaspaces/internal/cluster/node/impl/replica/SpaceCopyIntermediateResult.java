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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyResult;
import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.j_spaces.core.SpaceCopyStatus;
import com.j_spaces.core.SpaceCopyStatusImpl;
import com.j_spaces.kernel.JSpaceUtilities;

import net.jini.core.event.EventRegistration;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


@com.gigaspaces.api.InternalApi
public class SpaceCopyIntermediateResult
        implements ISpaceCopyIntermediateResult, ISpaceCopyResult {

    private final ConcurrentHashMap<String, AtomicInteger> _writtenTypesCount;
    private final ConcurrentHashMap<String, AtomicInteger> _notifyTemplateTypesCount;
    private final ConcurrentHashMap<String, String> _duplicateEntries;
    private final AtomicInteger _totalCopiedObjects;
    private final AtomicInteger _blockedByFilterEntries;
    private final Exception _failureReason;
    private final ConcurrentHashSet<EventRegistration> _notifyRegistrations;

    public SpaceCopyIntermediateResult() {
        _writtenTypesCount = new ConcurrentHashMap<String, AtomicInteger>();
        _notifyTemplateTypesCount = new ConcurrentHashMap<String, AtomicInteger>();
        _duplicateEntries = new ConcurrentHashMap<String, String>();
        _totalCopiedObjects = new AtomicInteger(0);
        _blockedByFilterEntries = new AtomicInteger(0);
        _notifyRegistrations = new ConcurrentHashSet<EventRegistration>();
        _failureReason = null;
    }

    public SpaceCopyIntermediateResult(
            Map<String, Integer> writtenTypesCount,
            Map<String, Integer> notifyTemplateTypesCount,
            Map<String, String> duplicateEntries,
            int totalCopiedObjects, int blockedByFilterEntries, Exception failureReason, ConcurrentHashSet<EventRegistration> notifyRegistrations) {
        _writtenTypesCount = toConcurrentCount(writtenTypesCount);
        _notifyTemplateTypesCount = toConcurrentCount(notifyTemplateTypesCount);
        _duplicateEntries = new ConcurrentHashMap<String, String>(duplicateEntries);
        _totalCopiedObjects = new AtomicInteger(totalCopiedObjects);
        _blockedByFilterEntries = new AtomicInteger(blockedByFilterEntries);
        _notifyRegistrations = notifyRegistrations;
        _failureReason = failureReason;
    }

    public ISpaceCopyIntermediateResult merge(
            ISpaceCopyIntermediateResult mergeWith) {
        SpaceCopyIntermediateResult other = (SpaceCopyIntermediateResult) mergeWith;
        Map<String, Integer> mergedWritten = mergeCountMaps(_writtenTypesCount, other._writtenTypesCount);
        Map<String, Integer> mergedNotify = mergeCountMaps(_notifyTemplateTypesCount, other._notifyTemplateTypesCount);
        int mergedTotalCopied = _totalCopiedObjects.get() + other._totalCopiedObjects.get();
        int mergedTotalBlocked = _blockedByFilterEntries.get() + other._blockedByFilterEntries.get();
        Map<String, String> mergedDuplicateEntries = new HashMap<String, String>(_duplicateEntries);
        mergedDuplicateEntries.putAll(other._duplicateEntries);
        Exception mergedFailureReason = _failureReason;
        if (mergedFailureReason == null)
            mergedFailureReason = other._failureReason;

        ConcurrentHashSet<EventRegistration> mergedRegistrations = new ConcurrentHashSet<EventRegistration>();
        mergedRegistrations.addAll(_notifyRegistrations);
        mergedRegistrations.addAll(other._notifyRegistrations);
        return new SpaceCopyIntermediateResult(mergedWritten, mergedNotify, mergedDuplicateEntries, mergedTotalCopied, mergedTotalBlocked, mergedFailureReason, mergedRegistrations);
    }

    private static Map<String, Integer> mergeCountMaps(
            ConcurrentHashMap<String, AtomicInteger> writtenTypesCount1,
            ConcurrentHashMap<String, AtomicInteger> writtenTypesCount2) {
        HashMap<String, Integer> result = new HashMap<String, Integer>(toFinalCountMap(writtenTypesCount1));
        for (Map.Entry<String, AtomicInteger> entry : writtenTypesCount2.entrySet()) {
            Integer currentCount = result.get(entry.getKey());
            if (currentCount == null)
                currentCount = 0;

            currentCount += entry.getValue().get();
            result.put(entry.getKey(), currentCount);
        }
        return result;
    }

    public ISpaceCopyIntermediateResult mergeFailure(Exception failureReason) {
        return new SpaceCopyIntermediateResult(toFinalCountMap(_writtenTypesCount),
                toFinalCountMap(_notifyTemplateTypesCount),
                _duplicateEntries,
                _totalCopiedObjects.get(),
                _blockedByFilterEntries.get(), failureReason, _notifyRegistrations);
    }

    private static Map<String, Integer> toFinalCountMap(
            ConcurrentHashMap<String, AtomicInteger> writtenTypesCount) {
        HashMap<String, Integer> result = new HashMap<String, Integer>();
        for (Map.Entry<String, AtomicInteger> entry : writtenTypesCount.entrySet()) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }

    private static ConcurrentHashMap<String, AtomicInteger> toConcurrentCount(
            Map<String, Integer> notifyTemplateTypesCount) {
        ConcurrentHashMap<String, AtomicInteger> result = new ConcurrentHashMap<String, AtomicInteger>();
        for (Map.Entry<String, Integer> entry : notifyTemplateTypesCount.entrySet()) {
            result.put(entry.getKey(), new AtomicInteger(entry.getValue()));
        }
        return result;
    }


    public ISpaceCopyResult toFinalResult() {
        return this;
    }

    public void increaseWritenTypeCount(String className) {
        increaseCount(className, _writtenTypesCount);
    }

    public void addDuplicateEntry(String uid, String className) {
        _duplicateEntries.put(uid, className);
    }

    public void incrementBlockedByFilterEntry() {
        _blockedByFilterEntries.incrementAndGet();
    }

    public void increaseNotifyTemplateTypeCount(String className) {
        increaseCount(className, _notifyTemplateTypesCount);
    }

    private void increaseCount(String className,
                               ConcurrentHashMap<String, AtomicInteger> countMap) {
        AtomicInteger counter = countMap.get(className);
        if (counter == null) {
            counter = new AtomicInteger();
            AtomicInteger prevCounter = countMap.putIfAbsent(className, counter);
            if (prevCounter != null)
                counter = prevCounter;
        }

        counter.incrementAndGet();
        _totalCopiedObjects.incrementAndGet();
    }


    public Exception getFailureReason() {
        return _failureReason;
    }

    public boolean isEmpty() {
        return _totalCopiedObjects.get() == 0;
    }

    public boolean isFailed() {
        return getFailureReason() != null;
    }

    public boolean isSuccessful() {
        return !isFailed();
    }


    public String getStringDescription(String remoteSpaceMember, String remoteMemberUrl, String spaceName, boolean spaceSyncOperation, long duration) {
        if (isFailed())
            return (spaceSyncOperation ? "Recovery" : "Copy") + " operation failed: " + getFailureReason().toString();

        StringBuilder sb = new StringBuilder();
        final String operation = spaceSyncOperation ? "Recovered" : "Copied";
        sb.append(operation).append(" from ").append(remoteSpaceMember).append(" [").append(remoteMemberUrl).append("] to ").append(spaceName);
        ///
        sb.append(" [duration=" + JSpaceUtilities.formatMillis(duration));
        sb.append(", total-objects=").append(_totalCopiedObjects.get());
        appendEntriesDesc(sb, _writtenTypesCount);
        appendNotifyTemplatesDesc(sb, _notifyTemplateTypesCount);
        appendDuplicatesDesc(sb, _duplicateEntries);
        int discarded = _blockedByFilterEntries.get();
        if (discarded != 0)
            sb.append(",discarded-by-replication-input-filter=" + discarded);
        sb.append("]");

        return sb.toString();
    }

    private static void append(StringBuilder sb, String format, Object... args) {
        String formatted = MessageFormat.format(format,
                args);
        sb.append(formatted);
    }

    private void appendEntriesDesc(StringBuilder sb, Map<String, AtomicInteger> copiedEntries) {
        if (copiedEntries.size() != 0) {
            sb.append(",entries={");
            int total = 0;
            for (Map.Entry<String, AtomicInteger> entry : copiedEntries.entrySet()) {
                int count = entry.getValue().get();
                sb.append(entry.getKey() + "=" + count + ",");
                total += count;
            }
            sb.append("TOTAL=" + total + "}");
        }
    }

    private void appendNotifyTemplatesDesc(StringBuilder sb, Map<String, AtomicInteger> copiedNotifyTemplates) {
        if (copiedNotifyTemplates.size() != 0) {
            sb.append(",notify-templates={");
            int total = 0;
            for (Map.Entry<String, AtomicInteger> entry : copiedNotifyTemplates.entrySet()) {
                int count = entry.getValue().get();
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

    public void addRegisteredNotifyTemplate(String templateClassName,
                                            GSEventRegistration notifyRegistration) {
        _notifyRegistrations.add(notifyRegistration);
        increaseCount(templateClassName, _notifyTemplateTypesCount);
    }

    public Set<EventRegistration> getNotifyRegistrations() {
        return _notifyRegistrations;
    }

    public SpaceCopyStatus toOldResult(short operationType, String targetMember) {
        SpaceCopyStatusImpl result = new SpaceCopyStatusImpl(operationType, targetMember);
        Set<Entry<String, AtomicInteger>> writtenTypesCount = _writtenTypesCount.entrySet();
        for (Entry<String, AtomicInteger> entry : writtenTypesCount) {
            String className = entry.getKey();
            AtomicInteger count = entry.getValue();
            result.setWriteCount(className, count.intValue());
        }
        Set<Entry<String, AtomicInteger>> notifyTemplateTypesCount = _notifyTemplateTypesCount.entrySet();
        for (Entry<String, AtomicInteger> entry : notifyTemplateTypesCount) {
            String className = entry.getKey();
            AtomicInteger count = entry.getValue();
            result.setNotifyTemplateCount(className, count.intValue());
        }
        Set<Entry<String, String>> duplicateEntries = _duplicateEntries.entrySet();
        for (Entry<String, String> entry : duplicateEntries) {
            String UID = entry.getKey();
            String className = entry.getValue();
            result.addDuplicateUID(className, UID);
        }
        result.setTotalDummyObj(_blockedByFilterEntries.intValue());
        if (_failureReason != null)
            result.setCauseException(_failureReason);
        return result;
    }

}
