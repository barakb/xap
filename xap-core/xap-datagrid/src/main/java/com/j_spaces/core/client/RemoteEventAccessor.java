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
 * @(#)RemoteEventAccessor.java 1.0  28/02/2005 17:14:36
 */

package com.j_spaces.core.client;

import com.gigaspaces.internal.transport.IEntryPacket;

import java.util.List;

/**
 * This utility class serves to access to the package/protected methods of EntryArrivedRemoteEvent
 * from other packages.
 *
 * @author Igor Goldenberg
 * @version 4.5
 **/
@com.gigaspaces.api.InternalApi
public class RemoteEventAccessor {
    /**
     * Initialize RemoteEvent with desired sequence number. Usually for MulticastNotifyDelegator
     * usage.
     **/
    public static void setRemoteEventSequence(EntryArrivedRemoteEvent theEvent, long sequenceNumber) {
        theEvent.setSequenceNumber(sequenceNumber);
    }

    /**
     * Returns <code>RemoteEvent</code> entry context.
     **/
    public static IEntryPacket getEventContextPacket(EntryArrivedRemoteEvent theEvent) {
        return theEvent.getEntryPacket();
    }

    /**
     * init the acceptableFilterList by desired capacity
     */
    public static void initAcceptableFilterList(EntryArrivedRemoteEvent theEvent, int capacity) {
        theEvent.initAcceptableFilterList(capacity);
    }

    /**
     * assume that initAcceptableFilterList() was called before, filterID or TemplateID
     */
    public static void addAcceptableFilterID(EntryArrivedRemoteEvent theEvent, String filterID) {
        theEvent.addAcceptableFilterID(filterID);
    }

    /**
     * returns AcceptableFilterList
     */
    public static List getAcceptableDestList(EntryArrivedRemoteEvent theEvent) {
        return theEvent.getAcceptableFilterList();
    }

}