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

package com.gigaspaces.internal.client.spaceproxy.metadata;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.entry.VirtualEntry;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.IdsQuery;
import com.j_spaces.core.client.EntrySnapshot;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.SQLQuery;

import net.jini.core.entry.Entry;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
public enum ObjectType {
    NULL, POJO, ENTRY, METADATA_ENTRY, EXTERNAL_ENTRY, DOCUMENT, SQL, ENTRY_SNAPSHOT, ENTRY_PACKET, TEMPLATE_PACKET, SPACE_TASK, ID_QUERY, IDS_QUERY;

    public static ObjectType fromObject(Object object) {
        return fromObject(object, false);
    }

    public static ObjectType fromObject(Object object, boolean supportIdsQuery) {
        if (object == null)
            return ObjectType.NULL;
        if (object instanceof ITransportPacket) {
            if (object instanceof ITemplatePacket)
                return ObjectType.TEMPLATE_PACKET;
            if (object instanceof IEntryPacket)
                return ObjectType.ENTRY_PACKET;
            throw new IllegalArgumentException("Unsupported ITransportPacket class: " + object.getClass().getName());
        }
        if (object instanceof VirtualEntry) {
            if (object instanceof SpaceDocument)
                return ObjectType.DOCUMENT;
            throw new IllegalArgumentException("Unsupported VirtualEntry class: " + object.getClass().getName());
        }
        if (object instanceof ISpaceQuery<?>) {
            if (object instanceof EntrySnapshot<?>)
                return ObjectType.ENTRY_SNAPSHOT;
            if (object instanceof SQLQuery<?>)
                return ObjectType.SQL;
            if (object instanceof IdQuery<?>)
                return ObjectType.ID_QUERY;
            if (supportIdsQuery && object instanceof IdsQuery<?>)
                return ObjectType.IDS_QUERY;
            throw new IllegalArgumentException("Unsupported ISpaceQuery class: " + object.getClass().getName());
        }

        if (object instanceof Entry) {
            if (object instanceof ExternalEntry)
                return ObjectType.EXTERNAL_ENTRY;
            return ObjectType.ENTRY;
        }

        return ObjectType.POJO;
    }

    public boolean isConcrete() {
        return this == POJO || this == ENTRY;
    }
}
