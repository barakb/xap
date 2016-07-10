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

package com.j_spaces.core;

import com.gigaspaces.internal.utils.StringUtils;

/**
 * Thrown when a space is considered unhealthy
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceUnhealthyException
        extends Exception {
    /** */
    private static final long serialVersionUID = 1L;

    private final Throwable[] causes;

    public SpaceUnhealthyException(Throwable[] causes) {
        super("Space is in unhealthy state: " +
                        ((causes == null || causes.length == 0) ? "" : toString(causes)),
                (causes == null || causes.length == 0) ? null : causes[causes.length - 1]);
        this.causes = causes;
    }

    private static String toString(Throwable[] causes) {
        StringBuilder b = new StringBuilder();

        for (Throwable t : causes) {
            b.append(t.getMessage() + StringUtils.NEW_LINE);
        }
        return b.toString();
    }

    /**
     * Get the causes for the space to be considered unhealthy
     *
     * @return causes
     */
    public Throwable[] getCauses() {
        return causes;
    }

}
