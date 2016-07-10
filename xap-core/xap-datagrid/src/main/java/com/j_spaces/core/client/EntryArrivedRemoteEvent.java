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

import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.events.SpaceRemoteEvent;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.nio.LRMIMethodTrackingIdProvider;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.OperationID;

import net.jini.core.entry.UnusableEntryException;
import net.jini.id.Uuid;
import net.jini.id.UuidFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.MarshalledObject;
import java.util.ArrayList;
import java.util.List;

/**
 * An EntryArrivedRemoteEvent is sent to a NotifyDelegator object when an entry that matches the
 * delegator's template enters a GigaSpace.
 *
 * @author Igor Goldenberg
 * @version 2.0
 */

public class EntryArrivedRemoteEvent extends SpaceRemoteEvent implements Cloneable, LRMIMethodTrackingIdProvider {
    private static final long serialVersionUID = -8397537059416989796L;

    private int _notifyType;
    protected String _templateUID;
    private QueryResultTypeInternal _resultType;
    private Uuid _spaceProxyUuid;
    private IEntryPacket _entryPacket;
    //private IEntryPacket _oldEntryPacket = null;
    private List<String> _acceptableFilterIDList;
    private boolean _fromReplication;

    protected transient IJSpace _spaceProxy;

    /**
     * Empty constructor.
     */
    public EntryArrivedRemoteEvent() {
        super();
    }

    /**
     * /** This constructor is reserved for internal usage only.
     */
    public EntryArrivedRemoteEvent(IJSpace source, long eventID, long seqNum, MarshalledObject handback,
                                   IEntryPacket entryPacket, IEntryPacket oldEntryPacket, NotifyActionType notifyType, boolean fromReplication, String templateUID,
                                   QueryResultTypeInternal resultType) {
        super(EMPTY_STRING, eventID, seqNum, handback);

        _notifyType = notifyType.getModifier();
        _entryPacket = initEntryPacket(entryPacket, oldEntryPacket, _notifyType);
        _spaceProxy = source;
        _spaceProxyUuid = _spaceProxy.getReferentUuid();
        _fromReplication = fromReplication;
        _templateUID = templateUID;
        _resultType = resultType;
    }

    private static IEntryPacket initEntryPacket(IEntryPacket entryPacket, IEntryPacket oldEntryPacket, int notifyType) {
        IEntryPacket result = entryPacket;
        if (notifyType == NotifyActionType.NOTIFY_UNMATCHED.getModifier()) {
            result = oldEntryPacket;
            if (oldEntryPacket.getTypeDescriptor() == null) {
                oldEntryPacket.setTypeDesc(entryPacket.getTypeDescriptor(), entryPacket.isSerializeTypeDesc());
            }
        }
        return result;
    }

    /**
     * Returns an internal GigaSpaces structure to send/receive entries from the space.
     *
     * @return the <code>EntryPacket</code>
     */
    public IEntryPacket getEntryPacket() {
        return _entryPacket;
    }

    /**
     * The space on which the Event initially occurred.
     *
     * @return The space proxy on which the Event initially occurred.
     */
    public Object getSource() {
        return _spaceProxy;
    }

    public OperationID getOperationID() {
        if (_entryPacket == null)
            return null;

        return _entryPacket.getOperationID();
    }

    /**
     * Returns the unique uid of the space the event originated from.
     *
     * @return the unique uid of the space the event originated from.
     */
    public Uuid getSpaceUuid() {
        return _spaceProxyUuid;
    }

    /**
     * Returns the entry by specification of user.
     *
     * @return the entry that arrived at the space.
     * @throws UnusableEntryException This exception is no longer thrown.
     */
    public Object getObject() throws UnusableEntryException {
        return _entryPacket.toObject(_resultType);
    }

    /**
     * Returns the entry that holds the values before the notify action was performed.
     *
     * @throws UnusableEntryException This exception is no longer thrown.
     */
    /*
    public Object getOldObject() throws UnusableEntryException {
        if (_notifyType == NotifyActionType.NOTIFY_TAKE.getModifier()) {
            return getObject();
        } else if (_notifyType == NotifyActionType.NOTIFY_UPDATE.getModifier() || _notifyType == NotifyActionType.NOTIFY_MATCHED_UPDATE.getModifier()
                || _notifyType == NotifyActionType.NOTIFY_REMATCHED_UPDATE.getModifier() || _notifyType == NotifyActionType.NOTIFY_UNMATCHED.getModifier()) {// old value
            if (_oldEntryPacket == null) {
                return null;
            } else {
                if (_oldEntryPacket.getTypeDescriptor() == null) {
                    _oldEntryPacket.setTypeDesc(_entryPacket.getTypeDescriptor(), _entryPacket.isSerializeTypeDesc());
                }
                return _oldEntryPacket.toObject(_resultType);
            }
        } else {// no old value exists
            return null;
        }
    }
    */
    /**
     * Returns the entry that holds the values after the notify action was performed.
     *
     * @throws UnusableEntryException This exception is no longer thrown.
     */
    /*
    public Object getNewObject() throws UnusableEntryException {
        if (_notifyType == NotifyActionType.NOTIFY_TAKE.getModifier()) {// no new value exists
            return null;
        } else if (_notifyType == NotifyActionType.NOTIFY_WRITE.getModifier() || _notifyType == NotifyActionType.NOTIFY_UPDATE.getModifier()
                || _notifyType == NotifyActionType.NOTIFY_MATCHED_UPDATE.getModifier() || _notifyType == NotifyActionType.NOTIFY_REMATCHED_UPDATE.getModifier()) {
            return getObject();
        } else if (_notifyType == NotifyActionType.NOTIFY_UNMATCHED.getModifier()) {
            if (_entryPacket.getTypeDescriptor() == null) {
                _entryPacket.setTypeDesc(_oldEntryPacket.getTypeDescriptor(), _oldEntryPacket.isSerializeTypeDesc());
            }
            return _entryPacket.toObject(_resultType);
        } else {
            return null;
        }
    }
    */

    /**
     * Returns a notify type of this event.
     *
     * @return Returns a notify type of this event.
     * @deprecated since 9.1 use {@link #getNotifyActionType()} instead.
     */
    @Deprecated
    public int getNotifyType() {
        return _notifyType;
    }

    /**
     * Returns a notify type of this event.
     *
     * @return notify type of this event.
     * @since 6.0
     */
    public NotifyActionType getNotifyActionType() {
        return NotifyActionType.fromModifier(_notifyType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MarshalledObject getRegistrationObject() {
        return handback;
    }

    void setHandback(MarshalledObject handback) {
        this.handback = handback;
    }

    public void setSpaceProxy(IJSpace spaceProxy) {
        _spaceProxy = spaceProxy;
        if (_entryPacket != null) {
            spaceProxy.getDirectProxy().getTypeManager().loadTypeDescToPacket(_entryPacket);
        }
    }

    /**
     * for MulticastNotifyWorker FIFO usage.
     */
    void setSequenceNumber(long sequenceNumber) {
        seqNum = sequenceNumber;
    }

    /**
     * Returns a shallow copy of this <tt>EntryArrivedRemoteEvent</tt> instance.
     *
     * @return A clone of this <tt>EntryArrivedRemoteEvent</tt> instance.
     */
    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    protected void setSpaceProxyUuid(Uuid spaceProxyUuid) {
        this._spaceProxyUuid = spaceProxyUuid;
    }

    /**
     * init the acceptableFilterList by desired capacity.
     */
    void initAcceptableFilterList(int capacity) {
        /** do lazy init */
        if (_acceptableFilterIDList == null && capacity > 0)
            _acceptableFilterIDList = new ArrayList<String>(capacity);
    }

    /**
     * assume that initAcceptableFilterList() was called before, add destination ID(FilterId or
     * TemplateID).
     */
    void addAcceptableFilterID(String destID) {
        /** double check that <code>_acceptableFilterIDList</code> is initialized */
        initAcceptableFilterList(1);

        _acceptableFilterIDList.add(destID);
    }

    /**
     * returns AcceptableFilterList.
     */
    List<?> getAcceptableFilterList() {
        return _acceptableFilterIDList;
    }

    /**
     * returns multicast templateID, TODO remote this support after support TemplateID ==
     * NotifySpaceUID
     */
    public String getTemplateUID() {
        return _templateUID;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (_spaceProxy != null)
            sb.append(" [Source space: ").append(_spaceProxy.getName()).append("]");

        sb.append("\n [ EventId: ").append(eventID).append("]");
        sb.append("\n [ SeqId: ").append(seqNum).append("]");
        sb.append("\n [ NotifyType: ").append(_notifyType).append("]");
        sb.append("\n [ SpaceUUID: ").append(_spaceProxyUuid).append("]");

        return sb.toString();
    }

    /**
     * @return true if event comes from replication, otherwise false.
     */
    public boolean isFromReplication() {
        return _fromReplication;
    }

    public String getKey() {
        if (_templateUID != null)
            return _templateUID;
        return Long.toString(getID());
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);
        /*if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            writeExternalV10_1(out);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v9_0_2))*/
        writeExternalV9_0_2(out);
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);
        /*if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            readExternalV10_1(in);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v9_0_2))*/
        readExternalV9_0_2(in);
    }

    private static final int FLAG_EVENT_ID = 1 << 0;
    private static final int FLAG_SEQ_NUM = 1 << 1;
    private static final int FLAG_NOTIFY_TYPE = 1 << 2;
    private static final int FLAG_TEMPLATE_UID = 1 << 3;
    private static final int FLAG_SPACE_PROXY_UUID = 1 << 4;
    private static final int FLAG_ENTRY_PACKET = 1 << 5;
    private static final int FLAG_HANDBACK = 1 << 6;
    private static final int FLAG_FROM_REPLICATION = 1 << 7;
    private static final int FLAG_FILTER_ID_LIST = 1 << 8;
    private static final int FLAG_RESULT_ENTRY_TYPE = 1 << 9;
    private static final int FLAG_OLD_ENTRY_PACKET = 1 << 10;

    private int buildFlags() {
        int flags = 0;

        if (eventID != 0)
            flags |= FLAG_EVENT_ID;
        if (seqNum != 0)
            flags |= FLAG_SEQ_NUM;
        if (_notifyType != 0)
            flags |= FLAG_NOTIFY_TYPE;
        if (_templateUID != null)
            flags |= FLAG_TEMPLATE_UID;
        if (_spaceProxyUuid != null)
            flags |= FLAG_SPACE_PROXY_UUID;
        if (_entryPacket != null)
            flags |= FLAG_ENTRY_PACKET;
        if (handback != null)
            flags |= FLAG_HANDBACK;
        if (_fromReplication)
            flags |= FLAG_FROM_REPLICATION;
        if (_acceptableFilterIDList != null && _acceptableFilterIDList.size() != 0)
            flags |= FLAG_FILTER_ID_LIST;
        if (_resultType != null)
            flags |= FLAG_RESULT_ENTRY_TYPE;
        //if (_oldEntryPacket != null)
        //    flags |= FLAG_OLD_ENTRY_PACKET;

        return flags;
    }

    /*
        private void writeExternalV10_1(ObjectOutput out) throws IOException {
            int flags = buildFlags();
            out.writeInt(flags);

            if (eventID != 0)
                out.writeLong(eventID);
            if (seqNum != 0)
                out.writeLong(seqNum);
            if (_notifyType != 0)
                out.writeInt(_notifyType);
            if (_templateUID != null)
                IOUtils.writeString(out, _templateUID);
            if (_resultType != null)
                out.writeByte(_resultType.getCode());

            if (_spaceProxyUuid != null) {
                out.writeLong(_spaceProxyUuid.getMostSignificantBits());
                out.writeLong(_spaceProxyUuid.getLeastSignificantBits());
            }
            if (_entryPacket != null)
                IOUtils.writeObject(out, _entryPacket);
            if (handback != null)
                IOUtils.writeObject(out, handback);
            if (_oldEntryPacket != null) {
                IOUtils.writeObject(out, _oldEntryPacket);
            }
            int filterLength = (_acceptableFilterIDList == null) ? 0 : _acceptableFilterIDList.size();
            if (filterLength != 0) {
                out.writeInt(filterLength);
                for (int i = 0; i < filterLength; i++) {
                    String filterId = _acceptableFilterIDList.get(i);
                    IOUtils.writeString(out, filterId);
                }
            }
            // force GC to clean the _acceptableDestIDList reference
            _acceptableFilterIDList = null;
        }

        private void readExternalV10_1(ObjectInput in) throws IOException, ClassNotFoundException {
            int flags = in.readInt();

            _fromReplication = ((flags & FLAG_FROM_REPLICATION) != 0);

            if ((flags & FLAG_EVENT_ID) != 0) {
                eventID = in.readLong();
            }
            if ((flags & FLAG_SEQ_NUM) != 0) {
                seqNum = in.readLong();
            }
            if ((flags & FLAG_NOTIFY_TYPE) != 0) {
                _notifyType = in.readInt();
            }
            if ((flags & FLAG_TEMPLATE_UID) != 0) {
                _templateUID = IOUtils.readString(in);
            }
            if ((flags & FLAG_RESULT_ENTRY_TYPE) != 0) {
                _resultType = QueryResultTypeInternal.fromCode(in.readByte());
            }

            if ((flags & FLAG_SPACE_PROXY_UUID) != 0) {
                long msb = in.readLong();
                long lsb = in.readLong();
                _spaceProxyUuid = UuidFactory.create(msb, lsb);
            }
            if ((flags & FLAG_ENTRY_PACKET) != 0) {
                _entryPacket = IOUtils.readObject(in);
            }
            if ((flags & FLAG_HANDBACK) != 0) {
                handback = IOUtils.readObject(in);
            }
            if ((flags & FLAG_OLD_ENTRY_PACKET) != 0) {
                _oldEntryPacket = IOUtils.readObject(in);
            }

            if ((flags & FLAG_FILTER_ID_LIST) != 0) {
                int filterLength = in.readInt();
                _acceptableFilterIDList = new ArrayList<String>(filterLength);
                for (int i = 0; i < filterLength; i++) {
                    String filterId = IOUtils.readString(in);
                    _acceptableFilterIDList.add(filterId);
                }
            }
        }
    */
    private void writeExternalV9_0_2(ObjectOutput out) throws IOException {
        int flags = buildFlags();
        out.writeInt(flags);

        if (eventID != 0)
            out.writeLong(eventID);
        if (seqNum != 0)
            out.writeLong(seqNum);
        if (_notifyType != 0)
            out.writeInt(_notifyType);
        if (_templateUID != null)
            IOUtils.writeString(out, _templateUID);
        if (_resultType != null)
            out.writeByte(_resultType.getCode());

        if (_spaceProxyUuid != null) {
            out.writeLong(_spaceProxyUuid.getMostSignificantBits());
            out.writeLong(_spaceProxyUuid.getLeastSignificantBits());
        }
        if (_entryPacket != null)
            IOUtils.writeObject(out, _entryPacket);
        if (handback != null)
            IOUtils.writeObject(out, handback);

        int filterLength = (_acceptableFilterIDList == null) ? 0 : _acceptableFilterIDList.size();
        if (filterLength != 0) {
            out.writeInt(filterLength);
            for (int i = 0; i < filterLength; i++) {
                String filterId = _acceptableFilterIDList.get(i);
                IOUtils.writeString(out, filterId);
            }
        }
        // force GC to clean the _acceptableDestIDList reference
        _acceptableFilterIDList = null;
    }

    private void readExternalV9_0_2(ObjectInput in) throws IOException, ClassNotFoundException {
        final int flags = in.readInt();

        _fromReplication = ((flags & FLAG_FROM_REPLICATION) != 0);

        if ((flags & FLAG_EVENT_ID) != 0)
            eventID = in.readLong();
        if ((flags & FLAG_SEQ_NUM) != 0)
            seqNum = in.readLong();
        if ((flags & FLAG_NOTIFY_TYPE) != 0)
            _notifyType = in.readInt();
        if ((flags & FLAG_TEMPLATE_UID) != 0)
            _templateUID = IOUtils.readString(in);
        if ((flags & FLAG_RESULT_ENTRY_TYPE) != 0)
            _resultType = QueryResultTypeInternal.fromCode(in.readByte());

        if ((flags & FLAG_SPACE_PROXY_UUID) != 0) {
            long msb = in.readLong();
            long lsb = in.readLong();
            _spaceProxyUuid = UuidFactory.create(msb, lsb);
        }
        if ((flags & FLAG_ENTRY_PACKET) != 0)
            _entryPacket = IOUtils.readObject(in);
        if ((flags & FLAG_HANDBACK) != 0)
            handback = IOUtils.readObject(in);

        if ((flags & FLAG_FILTER_ID_LIST) != 0) {
            int filterLength = in.readInt();
            _acceptableFilterIDList = new ArrayList<String>(filterLength);
            for (int i = 0; i < filterLength; i++) {
                String filterId = IOUtils.readString(in);
                _acceptableFilterIDList.add(filterId);
            }
        }
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "notify";
    }
}
