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


package org.openspaces.core.space.mode;

import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.j_spaces.core.IJSpace;

/**
 * A Space mode event that is raised before the space mode is changed to the space mode reflected in
 * this event. Note, this event might be called several times with the same space mode.
 *
 * @author kimchy
 */
public class BeforeSpaceModeChangeEvent extends AbstractSpaceModeChangeEvent {

    private static final long serialVersionUID = 1517730321537539772L;

    /**
     * Creates a new before space mode event.
     *
     * @param space     The space that changed its mode
     * @param spaceMode The current space mode (the one that it will change to)
     */
    public BeforeSpaceModeChangeEvent(IJSpace space, SpaceMode spaceMode) {
        super(space, spaceMode);
    }

    @Override
    public String toString() {
        return "BeforeSpaceModeChangeEvent[" + getSpaceMode() + "], Space [" + getSpace() + "]";
    }
}
