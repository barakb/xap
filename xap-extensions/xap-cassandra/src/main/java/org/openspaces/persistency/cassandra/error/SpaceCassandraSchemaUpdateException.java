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

package org.openspaces.persistency.cassandra.error;

/**
 * A runtime exception for Cassandra schema update related exceptions Contains an isRetryable field
 * denoting wheter this is an exception which might be resolved by retrying the operation.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class SpaceCassandraSchemaUpdateException
        extends SpaceCassandraException {
    private static final long serialVersionUID = 1L;

    private final boolean isRetryable;

    public SpaceCassandraSchemaUpdateException(String message, Throwable e, boolean isRetryable) {
        super(message, e);
        this.isRetryable = isRetryable;
    }

    public boolean isRetryable() {
        return isRetryable;
    }

}
