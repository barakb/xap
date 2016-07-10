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


package com.j_spaces.core.cluster.startup;


/**
 * An interface for objects that need to be notified about space connect/disconnect events
 *
 * @author anna
 * @version 1.0
 * @since 5.1
 */
public interface SpaceConnectionListener {
    /**
     * Called when given space connects
     *
     * @param event contains the connected space
     */
    void onSpaceConnect(SpaceConnectionEvent event);

    /**
     * Called when given space disconnects
     *
     * @param event contains the disconnected space
     */
    void onSpaceDisconnect(SpaceConnectionEvent event);

}
