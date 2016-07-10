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
 * A Space mode event that is raised after the space mode was changed to the space mode reflected in
 * this event. Note, this event might be called several times with the same space mode.
 *
 * @author kimchy
 */
public class AfterSpaceModeChangeEvent extends AbstractSpaceModeChangeEvent {

    private static final long serialVersionUID = 3684643308907987339L;

    /**
     * Creates a new after space mode event.
     *
     * @param space     The space that changed its mode
     * @param spaceMode The current space mode
     */
    public AfterSpaceModeChangeEvent(IJSpace space, SpaceMode spaceMode) {
        super(space, spaceMode);
    }

    @Override
    public String toString() {
        return "AfterSpaceModeChangeEvent[" + getSpaceMode() + "], Space [" + getSpace() + "]";
    }
}
