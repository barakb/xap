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

package com.gigaspaces.server.filter;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.events.NotifyContext;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.filters.entry.SpaceFilterEntryImpl;

/**
 * NotifyEvent is the object passed to filter when notify occurs
 *
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class NotifyEvent extends SpaceFilterEntryImpl {
    private static final long serialVersionUID = 9205896190317891054L;

    private final NotifyContext _notifyContext;

    /**
     * @param holder
     * @param tte
     */
    public NotifyEvent(IEntryHolder holder, ITypeDesc tte, NotifyContext notifyContext) {
        super(holder, tte);
        _notifyContext = notifyContext;
    }

    public NotifyContext getNotifyContext() {
        return _notifyContext;
    }
}
