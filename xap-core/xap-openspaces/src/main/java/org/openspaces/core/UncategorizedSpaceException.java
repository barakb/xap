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

import org.springframework.dao.DataAccessException;
import org.springframework.dao.UncategorizedDataAccessException;

/**
 * A GigaSpace based data access exception that could not be translated to one of Spring {@link
 * DataAccessException} subclasses or one of our own subclasses.
 *
 * @author kimchy
 */
public class UncategorizedSpaceException extends UncategorizedDataAccessException {

    private static final long serialVersionUID = -5799478121195480605L;

    public UncategorizedSpaceException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
