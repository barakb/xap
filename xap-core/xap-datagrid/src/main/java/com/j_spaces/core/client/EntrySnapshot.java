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

import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.query.ISpaceQuery;

/**
 * Entry snapshot class.
 *
 * @author Igor Goldenberg
 * @version 1.0
 */
@com.gigaspaces.api.InternalApi
public class EntrySnapshot<T> implements ISpaceQuery<T>, net.jini.core.entry.Entry {
    private static final long serialVersionUID = 1L;

    private final ITemplatePacket _packet;

    public EntrySnapshot(ITemplatePacket packet) {
        _packet = packet;
    }

    public ITemplatePacket getTemplatePacket() {
        return _packet;
    }
}
