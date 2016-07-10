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

package com.gigaspaces.internal.server.space.metadata;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;

/**
 * Listener interface for enhancing server type descriptor life cycle with custom code.
 *
 * @author Niv Ingberg
 * @since 8.0
 */
public interface IServerTypeDescListener {
    /**
     * Called after a new type descriptor was created but before it it visible. Note: This method is
     * invoked in a synchronized context.
     */
    void onTypeAdded(IServerTypeDesc typeDesc);

    /**
     * Called after a new type descriptor was activated. Note: This method is invoked in a
     * synchronized context.
     */
    void onTypeActivated(IServerTypeDesc typeDesc);

    void onTypeDeactivated(IServerTypeDesc typeDesc);

    /**
     * TODO: Called after index(s) were added to a type descriptor. Note: This method is invoked in
     * a synchronized context.
     */
    void onTypeIndexAdded(IServerTypeDesc typeDesc);
}
