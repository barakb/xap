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

package com.gigaspaces.events;

import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;

import net.jini.core.event.EventRegistration;
import net.jini.core.lease.Lease;
import net.jini.id.Uuid;

import java.util.HashMap;
import java.util.Map;

/**
 * @author assafr
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class GSEventRegistration extends EventRegistration implements Textualizable {
    private static final long serialVersionUID = -432048121117103185L;

    private final String _templateID;
    private final Uuid _spaceUID;
    private Map<Uuid, Long> _sequenceNumbers;

    public GSEventRegistration(long eventID, Object source, Lease lease, long seqNum) {
        this(eventID, source, lease, seqNum, null, null);
    }

    public GSEventRegistration(long eventID, Object source, Lease lease, long seqNum, String templateID, Uuid spaceUID) {
        super(eventID, source, lease, seqNum);
        this._templateID = templateID;
        this._spaceUID = spaceUID;
    }

    public void setLease(Lease lease) {
        super.lease = lease;
    }

    public String getTemplateID() {
        return _templateID;
    }

    public Uuid getSpaceUID() {
        return _spaceUID;
    }

    public String getKey() {
        if (_templateID != null) {
            return _templateID;
        } else {
            return Long.toString(getID());
        }
    }

    /**
     * when in partitioned space scenario, multiple EventRegistration objects are received, each
     * with his own sequence number. all the sequence numbers are accumulated here so that the fifo
     * delegator will know from what index (sequence number) to start.
     *
     * @param registration the EventRegistration from a single space.
     */
    public void addSequenceNumber(GSEventRegistration registration) {
        if (_sequenceNumbers == null)
            _sequenceNumbers = new HashMap<Uuid, Long>();

        _sequenceNumbers.put(registration.getSpaceUID(), registration.getSequenceNumber());
    }

    public Map<Uuid, Long> getSequenceNumbers() {
        if (_sequenceNumbers != null)
            return _sequenceNumbers;

        HashMap<Uuid, Long> map = new HashMap<Uuid, Long>();
        map.put(getSpaceUID(), getSequenceNumber());
        return map;
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("eventID", super.eventID);
        textualizer.append("source", super.source);
        textualizer.append("lease", super.lease);
        textualizer.append("seqNum", super.seqNum);
        textualizer.append("templateID", _templateID);
        textualizer.append("spaceUID", _spaceUID);
    }
}
