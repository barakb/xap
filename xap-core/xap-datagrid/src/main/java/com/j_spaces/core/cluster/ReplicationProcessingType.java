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

package com.j_spaces.core.cluster;

/**
 * Specifies the replication processing type
 *
 * @author eitany
 * @since 8.0
 */
public enum ReplicationProcessingType {
    /**
     * The replication packets have a global order that must be kept during processing (the
     * processing is single threaded)
     */
    GLOBAL_ORDER,
    /**
     * The replication packets are placed into different buckets that specify the processing order
     * (the processing is multi threaded)
     */
    MULTIPLE_BUCKETS,
    /**
     * The replication packets have a global order that must be kept during processing (the
     * processing is single threaded) Multiple participant packets can be skipped and executed when
     * all participant packets arrive. In the meantime, the next packets are executed until a packet
     * is dependent on a packet that was skipped.
     */
    MULTIPLE_SOURCES
}
