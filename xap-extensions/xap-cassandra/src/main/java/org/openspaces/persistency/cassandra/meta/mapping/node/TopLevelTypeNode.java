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

package org.openspaces.persistency.cassandra.meta.mapping.node;

/**
 * A {@link CompoundTypeNode} denoting a top level element. i.e. when an entry is read/written
 * from/to cassandra top level type node representing the entry metadata
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public interface TopLevelTypeNode extends CompoundTypeNode, ExternalizableTypeNode {

    /**
     * @return The type name matching this top level node.
     */
    String getTypeName();

    /**
     * @return The key name to be used in Cassandra for this type (in space terminology: the type id
     * property name).
     */
    String getKeyName();

    /**
     * @return The key type.
     */
    Class<?> getKeyType();

}
