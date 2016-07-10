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

package org.openspaces.persistency.cassandra.meta.conversion;

/**
 * An interface for converting type names to a matching Cassandra column family names. Note to
 * implementations: The returned name's length should not exceed 48, should not contain dots and
 * should only contain valid filename characters.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public interface ColumnFamilyNameConverter {

    /**
     * @param typeName The type name matching the column family name.
     * @return The column family name as it will exist on Cassandra.
     */
    String toColumnFamilyName(String typeName);

}
