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

package com.gigaspaces.internal.cluster.node.impl;


/**
 * Represents replication operation type, used internally by the {@link ReplicationNode}
 *
 * @author eitany
 * @since 8.0
 */
public enum ReplicationSingleOperationType {
    WRITE, UPDATE, REMOVE_ENTRY,
    CANCEL_LEASE, EVICT,
    INSERT_NOTIFY_TEMPLATE, REMOVE_NOTIFY_TEMPLATE,
    EXTEND_ENTRY_LEASE, EXTEND_NOTIFY_TEMPLATE_LEASE, DISCARD, DATA_TYPE_INTRODUCE, DATA_TYPE_ADD_INDEX,
    ENTRY_LEASE_EXPIRED, NOTIFY_TEMPLATE_LEASE_EXPIRED, CHANGE;

}
