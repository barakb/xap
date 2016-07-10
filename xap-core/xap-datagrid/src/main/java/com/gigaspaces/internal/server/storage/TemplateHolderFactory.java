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

package com.gigaspaces.internal.server.storage;

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;

/**
 * Factory to create TemplateHolder instances.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class TemplateHolderFactory {
    protected TemplateHolderFactory() {
    }

    public static ITemplateHolder createEmptyTemplateHolder(SpaceEngine engine,
                                                            String uid, long expirationTime, boolean isFifo) {
        return new TemplateHolder(
                engine.getTypeManager().getServerTypeDesc(IServerTypeDesc.ROOT_TYPE_NAME),
                new TemplatePacket(),
                uid,
                expirationTime,
                null					/* xidOriginated */,
                SystemTime.timeMillis()	/* SCN */,
                SpaceOperations.READ 	/* templateOperation */,
                null					/* respContext */,
                false					/* returnOnlyUid */,
                0						/* operationModifiers */,
                isFifo);
    }

    public static ITemplateHolder createTemplateHolder(IServerTypeDesc typeDesc,
                                                       ITemplatePacket template, String uid, long expirationTime) {
        return new TemplateHolder(typeDesc, template, uid, expirationTime,
                null					/* xidOriginated */,
                SystemTime.timeMillis()	/* SCN */,
                SpaceOperations.READ 	/* templateOperation */,
                null					/* respContext */,
                false					/* returnOnlyUid */,
                0						/* operationModifiers */,
                false					/* isFifo*/);
    }

    public static ITemplateHolder createTemplateHolder(IServerTypeDesc typeDesc,
                                                       ITemplatePacket template, String uid, long expirationTime,
                                                       XtnEntry xidOriginated, long SCN, int templateOperation,
                                                       IResponseContext respContext, boolean returnOnlyUid,
                                                       int operationModifiers, boolean isfifo) {
        return createTemplateHolder(typeDesc, template, uid, expirationTime, xidOriginated, SCN, templateOperation,
                respContext, returnOnlyUid, operationModifiers, isfifo, false);
    }

    public static ITemplateHolder createTemplateHolder(IServerTypeDesc typeDesc,
                                                       ITemplatePacket template, String uid, long expirationTime,
                                                       XtnEntry xidOriginated, long SCN, int templateOperation,
                                                       IResponseContext respContext, boolean returnOnlyUid,
                                                       int operationModifiers, boolean isfifo, boolean fromReplication) {
        return new TemplateHolder(typeDesc, template, uid, expirationTime,
                xidOriginated, SCN, templateOperation, respContext,
                returnOnlyUid, operationModifiers, isfifo, fromReplication);
    }

    public static NotifyTemplateHolder createNotifyTemplateHolder(IServerTypeDesc typeDesc,
                                                                  ITemplatePacket template, String templateUid, long expirationTime,
                                                                  long eventId, NotifyInfo notifyInfo, boolean isFifo) {
        return new NotifyTemplateHolder(typeDesc, template, templateUid, expirationTime, eventId, notifyInfo, isFifo);
    }

    /**
     * Special builder method for update that uses TemplateHolder instead EntryHolder
     */
    public static ITemplateHolder createUpdateTemplateHolder(IServerTypeDesc typeDesc,
                                                             IEntryPacket template, String uid, long expirationTime,
                                                             XtnEntry xidOriginated, long SCN, IResponseContext respContext,
                                                             int operationModifiers) {
        return new TemplateHolder(typeDesc, template, uid, expirationTime,
                xidOriginated, SCN, SpaceOperations.UPDATE, respContext,
                operationModifiers);
    }

    /**
     * Special builder method for reading object before replicating
     */
    public static ITemplateHolder createTemplateHolderForReplication(IServerTypeDesc typeDesc,
                                                                     IEntryPacket template, String uid, long expirationTime) {
        return new TemplateHolder(typeDesc, template, uid, expirationTime,
                null					/* xidOriginated */,
                SystemTime.timeMillis()	/* SCN */,
                SpaceOperations.READ 	/* templateOperation */,
                null					/* respContext */,
                0						/* operationModifiers */);
    }
}
