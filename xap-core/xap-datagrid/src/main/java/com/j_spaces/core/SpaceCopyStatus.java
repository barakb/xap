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



/*
 * @(#)SpaceCopyStatus.java 1.0   27.02.2006  17:50:19
 */

package com.j_spaces.core;

import com.j_spaces.core.client.SpaceURL;

import net.jini.core.event.EventRegistration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Contains all information about space copy operation.
 *
 * @author Igor Goldenberg
 * @see com.j_spaces.core.admin.IRemoteJSpaceAdmin#spaceCopy(IJSpace, Object, boolean, int)
 * @since 5.01
 **/
public interface SpaceCopyStatus extends Serializable {
    public int getTotalDummyObj();

    public void setTotalDummyObj(int totalDummyObj);

    public void incrementTotalDummyObjects();

    public int incrementCounter();

    /**
     * @return Total copied object from target member including notify templates.
     **/
    public int getTotalCopyObj();

    /**
     * @return The source member name from where all data have been copied.
     **/
    public String getSourceMemberName();

    /**
     * @return The target member name where all data have been copied to.
     **/
    public String getTargetMemberName();

    /**
     * @return Source member URL.
     **/
    public SpaceURL getSourceMemberURL();

    /**
     * @return Cause exception happened during space-copy operation.
     **/
    public Exception getCauseException();

    /**
     * @return Total blocked( IReplicationFilterEntry.discard() ) entries by Replication Input
     * Filter.
     **/
    public int getTotalBlockedEntriesByFilter();

    /**
     * @return Status of copied entries where Map.Entry.getKey(): classname and
     * Map.Entry.getValue(): count.
     **/
    public Map<String, Integer> getTotalCopiedEntries();

    /**
     * @return Status of copied templates where Map.Entry.getKey(): classname and
     * Map.Entry.getValue(): count. NOTE: The key of notify <code>null</code> templates is presented
     * as String: NULL_NOTIFY_TEMPLATE.
     **/
    public Map<String, Integer> getTotalCopiedNotifyTemplates();

    /**
     * @return Status of duplicate UID's already exist in target space. Key - UID, value -
     * className.
     **/
    public Map<String, String> getTotalDuplicateEntries();

    public HashMap<String, Integer> getWriteEntries();

    public HashMap<String, String> getDuplicateUID();

    public HashSet<EventRegistration> getNotifyTemplRegistration();
}