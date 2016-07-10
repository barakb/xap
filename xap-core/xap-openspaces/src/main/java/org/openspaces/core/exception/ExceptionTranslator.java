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


package org.openspaces.core.exception;

import org.springframework.dao.DataAccessException;

/**
 * Translates a low level JavaSpaces/Jini exception into a {@link DataAccessException} runtime
 * exception.
 *
 * @author kimchy
 */
public interface ExceptionTranslator {

    /**
     * Translates a low level exception into a {@link DataAccessException} runtime exception.
     *
     * @param e The low level exception to translate
     * @return The translated exception
     */
    DataAccessException translate(Throwable e);

    DataAccessException translateNoUncategorized(Throwable e);
}
