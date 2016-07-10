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


package org.openspaces.core;

import org.springframework.dao.InvalidDataAccessResourceUsageException;

/**
 * Thrown when failed to serialize or deserialize Entry field. Wraps {@link
 * com.j_spaces.core.EntrySerializationException}.
 *
 * @author kimchy
 */
public class EntrySerializationException extends InvalidDataAccessResourceUsageException {

    private static final long serialVersionUID = -200863207916463960L;

    public EntrySerializationException(com.j_spaces.core.EntrySerializationException e) {
        super(e.getMessage(), e);
    }
}
