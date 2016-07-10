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

/**
 * Title:		ISLHolder.java Description: Company:		GigaSpaces Technologies
 *
 * @author Moran Avigdor
 * @version 1.0 20/06/2005
 * @since 4.1 Build#
 */
package com.j_spaces.kernel;


/**
 * A cursor holder of a StoredList for each scan. This class is used in order to return and request
 * (during scan) information from the list.
 */
public interface IStoredListIterator<T> {
    /**
     * Sets the subject pointed to by the cursor's scan.
     *
     * @param subject The subject to set.
     */
    public abstract void setSubject(T subject);

    /**
     * Gets the subject pointed to by the cursor's scan.
     *
     * @return Returns the subject.
     */
    public abstract T getSubject();

    /**
     * Release of this SLHolder resource
     *
     * @see com.j_spaces.kernel.pool.Resource#release()
     */
    public abstract void release();

}