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


package com.gigaspaces.events;


import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerObjectMap;
import com.j_spaces.core.client.NotifyModifiers;

/**
 * An enum that specify a single notify action type such as write or update <br> and can also be
 * used to composite a complex action type such as write OR update <br> <code> NotifyActionType
 * updateOrWrite = {@link #NOTIFY_UPDATE}.or({@link #NOTIFY_WRITE}); </code>
 *
 * @since 6.0
 */

public class NotifyActionType {
    private static final IntegerObjectMap<NotifyActionType> values = CollectionsFactory.getInstance().createIntegerObjectMap();
    private int modifier;

    private NotifyActionType(int modifier) {
        this(modifier, true);
    }

    private NotifyActionType(int modifier, boolean basic) {
        this.modifier = modifier;
        if (basic)
            values.put(modifier, this);
    }

    public static NotifyActionType fromModifier(int modifier) {
        NotifyActionType type = values.get(modifier);
        if (type == null) {
            type = new Set(modifier);
        }

        return type;
    }

    protected void setModifier(int modifier) {
        this.modifier = modifier;
    }

    public int getModifier() {
        return modifier;
    }

    public NotifyActionType or(NotifyActionType type) {
        Set set = new Set(this);
        set.or(type);
        return set;
    }

    @Override
    public String toString() {
        return NotifyModifiers.toString(modifier);
    }

    public static final NotifyActionType NOTIFY_NONE = new NotifyActionType(NotifyModifiers.NOTIFY_NONE);
    public static final NotifyActionType NOTIFY_UPDATE = new NotifyActionType(NotifyModifiers.NOTIFY_UPDATE);
    public static final NotifyActionType NOTIFY_WRITE = new NotifyActionType(NotifyModifiers.NOTIFY_WRITE);
    public static final NotifyActionType NOTIFY_TAKE = new NotifyActionType(NotifyModifiers.NOTIFY_TAKE);
    public static final NotifyActionType NOTIFY_LEASE_EXPIRATION = new NotifyActionType(NotifyModifiers.NOTIFY_LEASE_EXPIRATION);
    public static final NotifyActionType NOTIFY_UNMATCHED = new NotifyActionType(NotifyModifiers.NOTIFY_UNMATCHED);
    public static final NotifyActionType NOTIFY_MATCHED_UPDATE = new NotifyActionType(NotifyModifiers.NOTIFY_MATCHED_UPDATE);
    public static final NotifyActionType NOTIFY_REMATCHED_UPDATE = new NotifyActionType(NotifyModifiers.NOTIFY_REMATCHED_UPDATE);


    public static final NotifyActionType NOTIFY_WRITE_OR_UPDATE = NOTIFY_WRITE.or(NOTIFY_UPDATE);
    /**
     * Modifier which includes the <code>NOTIFY_WRITE</code>, <code>NOTIFY_UPDATE</code>,
     * <code>NOTIFY_TAKE</code> and <code>NOTIFY_LEASE_EXPIRATION</code> modifiers.
     *
     * @deprecated since 9.6 - register using specific modifiers instead.
     */
    @Deprecated
    public static final NotifyActionType NOTIFY_ALL = new NotifyActionType(NotifyModifiers.NOTIFY_ALL);

    /**
     * if true match is done by UID only when UID is provided.
     */
    public static final NotifyActionType NOTIFY_MATCH_BY_ID = new NotifyActionType(NotifyModifiers.NOTIFY_MATCH_BY_ID);

    public boolean isUpdate() {
        return isModifierSet(NOTIFY_UPDATE);
    }

    public boolean isUnmatched() {
        return isModifierSet(NOTIFY_UNMATCHED);
    }

    public boolean isMatchedUpdate() {
        return isModifierSet(NOTIFY_MATCHED_UPDATE);
    }

    public boolean isRematchedUpdate() {
        return isModifierSet(NOTIFY_REMATCHED_UPDATE);
    }

    public boolean isWrite() {
        return isModifierSet(NOTIFY_WRITE);
    }

    public boolean isTake() {
        return isModifierSet(NOTIFY_TAKE);
    }

    public boolean isMatchByID() {
        return isModifierSet(NOTIFY_MATCH_BY_ID);
    }

    public boolean isLeaseExpiration() {
        return isModifierSet(NOTIFY_LEASE_EXPIRATION);
    }

    private boolean isModifierSet(NotifyActionType modifier) {
        return (getModifier() & modifier.getModifier()) == modifier.getModifier();
    }

    private static class Set extends NotifyActionType {

        public Set(int modifier) {
            super(modifier, false);
        }

        private Set(NotifyActionType action) {
            this(action.getModifier());
        }

        @Override
        public Set or(NotifyActionType actionType) {
            setModifier(getModifier() | actionType.getModifier());
            return this;
        }
    }

}
