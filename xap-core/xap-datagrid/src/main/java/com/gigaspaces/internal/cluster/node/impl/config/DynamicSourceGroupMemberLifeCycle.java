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

package com.gigaspaces.internal.cluster.node.impl.config;

/**
 * @author Dan Kilman
 * @since 9.0
 */
public abstract class DynamicSourceGroupMemberLifeCycle {
    private static final Object[] EMPTY_ARGS = new Object[0];

    /**
     * Called before target member is added
     *
     * @param memberAddedEvent The member added event
     */
    public void beforeMemberAdded(MemberAddedEvent memberAddedEvent) {
    }

    /**
     * Called after target member is added
     *
     * @param memberAddedEvent The member added event
     */
    public void afterMemberAdded(MemberAddedEvent memberAddedEvent) {
    }

    /**
     * Called after target member is removed
     */
    public void afterMemberRemoved(MemberRemovedEvent memberRemovedEvent) {

    }

    /**
     * Arguments need to construct the lifeCycle handler
     */
    public Object[] getConstructionArguments() {
        return EMPTY_ARGS;
    }
}
