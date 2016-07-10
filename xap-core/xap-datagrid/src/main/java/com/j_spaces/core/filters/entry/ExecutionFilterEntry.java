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

package com.j_spaces.core.filters.entry;

import com.j_spaces.core.IJSpace;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;

import java.io.IOException;
import java.rmi.MarshalledObject;
import java.util.Map;

/**
 * represent a task wrapper used inside a filter.
 *
 * @author asy ronen.
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class ExecutionFilterEntry implements ISpaceFilterEntry {
    private static final long serialVersionUID = 6656175001514819275L;

    final private Object task;

    public ExecutionFilterEntry(Object task) {
        this.task = task;
    }

    public MarshalledObject getHandback() {
        try {
            return new MarshalledObject(task);
        } catch (IOException e) {
            return null;
        }
    }

    public int getNotifyType() {
        return 0;
    }

    public Map.Entry getMapEntry() {
        return null;
    }

    public String getUID() {
        return null;
    }

    public String getClassName() {
        return task.getClass().getName();
    }

    public String[] getSuperClassesNames() {
        return null;
    }

    public String getCodebase() {
        return null;
    }

    public String[] getFieldsNames() {
        return null;
    }

    public String[] getFieldsTypes() {
        return null;
    }

    public Object[] getFieldsValues() {
        return null;
    }

    public boolean[] getIndexIndicators() {
        return null;
    }

    public String getPrimaryKeyName() {
        return null;
    }

    public boolean isFifo() {
        return false;
    }

    public boolean isTransient() {
        return false;
    }

    public boolean isReplicatable() {
        return false;
    }

    public long getTimeToLive() {
        return 0;
    }

    public int getVersion() {
        return 0;
    }

    public int getFieldPosition(String fieldName) {
        return 0;
    }

    public Object getFieldValue(String fieldName) throws IllegalArgumentException, IllegalStateException {
        return null;
    }

    public Object getFieldValue(int position) throws IllegalArgumentException, IllegalStateException {
        return null;
    }

    public Object setFieldValue(String fieldName, Object value) throws IllegalArgumentException, IllegalStateException {
        return null;
    }

    public Object setFieldValue(int position, Object value) throws IllegalArgumentException, IllegalStateException {
        return null;
    }

    public String getFieldType(String fieldName) throws IllegalArgumentException, IllegalStateException {
        return null;
    }

    public boolean isIndexedField(String fieldName) throws IllegalArgumentException, IllegalStateException {
        return false;
    }

    @Deprecated
    public Entry getEntry(IJSpace space) throws UnusableEntryException {
        return null;
    }

    public Object getObject(IJSpace space) throws UnusableEntryException {
        return task;
    }

    public String getRoutingFieldName() {
        return null;
    }
}
