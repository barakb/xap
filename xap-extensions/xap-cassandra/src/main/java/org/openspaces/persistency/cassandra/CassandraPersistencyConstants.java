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

package org.openspaces.persistency.cassandra;


/**
 * Contains constants values used in the Cassandra persistency layer implementation.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class CassandraPersistencyConstants {

    /**
     * Columns containing a {@link String} value with this value as their prefix will be considered
     * compound {@link com.gigaspaces.document.SpaceDocument} entries. Their type name will be
     * inferred from what follows after the colon.
     */
    public static final String SPACE_DOCUMENT_COLUMN_PREFIX = "__sd:";

    /**
     * Columns containing a {@link String} value with this value as their prefix will be considered
     * compound {@code POJO} entries. Their type will be inferred from what follows after the
     * colon.
     */
    public static final String POJO_ENTRY_COLUMN_PREFIX = "__pe:";

    /**
     * Columns containing a {@link String} value with this value as their prefix will be considered
     * compound {@link java.util.Map} entries.
     */
    public static final String MAP_ENTRY_COLUMN = "__me:";

}
