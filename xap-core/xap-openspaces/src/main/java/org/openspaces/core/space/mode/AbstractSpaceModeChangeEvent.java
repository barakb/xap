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

import org.springframework.context.ApplicationEvent;

/**
 * Base class for different space mode events.
 *
 * @author kimchy
 */
public abstract class AbstractSpaceModeChangeEvent extends ApplicationEvent {

    private static final long serialVersionUID = 6333546136563910455L;

    private SpaceMode spaceMode;

    /**
     * Creates a new Space mode event.
     *
     * @param space     The space that changed its mode
     * @param spaceMode The space mode of the space
     */
    public AbstractSpaceModeChangeEvent(IJSpace space, SpaceMode spaceMode) {
        super(space);
        this.spaceMode = spaceMode;
    }

    /**
     * Returns the space that initiated this event.
     */
    public IJSpace getSpace() {
        return (IJSpace) getSource();
    }

    public SpaceMode getSpaceMode() {
        return this.spaceMode;
    }

    /**
     * The space mode is <code>NONE</code>, in other words - unknown.
     */
    public boolean isNone() {
        return spaceMode == SpaceMode.NONE;
    }

    /**
     * The space mode is <code>BACKUP</code>.
     */
    public boolean isBackup() {
        return spaceMode == SpaceMode.BACKUP;
    }

    /**
     * The space mode is <code>PRIMARY</code>.
     */
    public boolean isPrimary() {
        return spaceMode == SpaceMode.PRIMARY;
    }
}
