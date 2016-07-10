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

import com.gigaspaces.document.DocumentObjectConverter;
import com.gigaspaces.metadata.SpaceDocumentSupport;

/**
 * A {@link DocumentObjectConverter} that does not fail if a type is missing during conversion.
 * Instead, it will return the original document.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class CassandraDocumentObjectConverter extends DocumentObjectConverter {

    public CassandraDocumentObjectConverter() {
        super(false);
    }

    @Override
    public Object fromDocumentIfNeeded(Object object, SpaceDocumentSupport documentSupport, Class<?> expectedType) {
        return super.fromDocumentIfNeeded(object, documentSupport, expectedType);
    }

}
