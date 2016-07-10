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


package com.j_spaces.core.client;

/**
 * @author Yechiel Fefer
 * @version 3.2
 * @see ClientUIDHandler
 * @deprecated Since 8.0 - This class is irrelevant since com.j_spaces.core.client.MetaDataEntry is
 * no longer supported,  Use POJO annotations instead.
 **/
@Deprecated
final public class EntryInfo {
    /**
     * Unique ID of the entry.
     */
    public String m_UID;

    /**
     * Version ID, initially each entry is written with versionID of 1 and it is incremented in each
     * update. VesrionID is used in optimistic locking.
     **/
    public int m_VersionID;

    /**
     * output-only, time (in milliseconds) left for this entry to live (correct for the time the
     * operation- read,take under xtn, update, ... was performed)
     **/
    public long m_TimeToLive;

    /**
     * Constructor.
     *
     * @param UID       unique ID of the entry
     * @param versionID entry version (used in optimistic locking)
     */
    public EntryInfo(String UID, int versionID) {
        m_UID = UID;
        m_VersionID = versionID;
    }

    /**
     * Constructor - for internal use only.
     *
     * @param UID        entry unique ID of the entry
     * @param versionID  entry version (used in optimistic locking)
     * @param timeToLive time for this entry to live
     */
    public EntryInfo(String UID, int versionID, long timeToLive) {
        this(UID, versionID);
        m_TimeToLive = timeToLive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "UID: " + m_UID + " VersionID: " + m_VersionID;
    }


    public String getUID() {
        return m_UID;
    }

    public int getVersionID() {
        return m_VersionID;
    }

    public long getTimeToLive() {
        return m_TimeToLive;
    }
}