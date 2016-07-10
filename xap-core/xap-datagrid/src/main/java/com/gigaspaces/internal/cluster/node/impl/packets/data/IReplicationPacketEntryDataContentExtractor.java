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

package com.gigaspaces.internal.cluster.node.impl.packets.data;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.server.storage.IEntryData;

/**
 * Used to extract content from {@link IReplicationPacketEntryData}
 *
 * @author eitany
 * @since 8.0.5
 */
public interface IReplicationPacketEntryDataContentExtractor {
    IEntryData getMainEntryData(IReplicationPacketEntryData data);

    IEntryData getSecondaryEntryData(IReplicationPacketEntryData data);

    String getMainTypeName(IReplicationPacketEntryData data);

    /**
     * Sets the given entry data to be serialized with full content when serialized
     */
    void setSerializeWithFullContent(IReplicationPacketEntryData entryData);

    boolean requiresConversion(IReplicationPacketEntryData entryData, QueryResultTypeInternal queryResultType);

    <T> T getCustomContent(IReplicationPacketEntryData entryData);

}
