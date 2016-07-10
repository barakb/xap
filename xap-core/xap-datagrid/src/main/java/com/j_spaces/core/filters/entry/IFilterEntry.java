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

import com.j_spaces.core.IGSEntry;

import java.rmi.MarshalledObject;

/**
 * This interface is common to all the entries that are passed to any of the space filters.
 *
 * @author Guy Korland
 * @version 1.0
 * @see com.j_spaces.core.filters.ISpaceFilter
 * @see com.j_spaces.core.cluster.IReplicationFilter
 * @since 5.0
 */
public interface IFilterEntry extends IGSEntry {
    /**
     * Gets the callback handback.<p> Notice:  only relevant if this entry represents a notify
     * template (see {@link #getNotifyType()}.
     *
     * @return the callback handback
     */
    MarshalledObject getHandback();

    /**
     * Gets the notify type.
     *
     * @return the notify type, if it's not a notify template a {@link com.j_spaces.core.client.NotifyModifiers#NOTIFY_NONE
     * NOTIFY_NONE} is returned.
     * @see com.j_spaces.core.client.NotifyModifiers
     */
    int getNotifyType();
}
