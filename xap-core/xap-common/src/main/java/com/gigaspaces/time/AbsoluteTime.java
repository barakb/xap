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

/*
 * Title:		 AbsoluteTime.java
 * Description: Default implementation of ITimeProvider interface.
 * Company:		 GigaSpaces Technologies
 * 
 * @author		 Moran Avigdor
 * @version		 1.0 12/12/2005
 * @since		 5.0EAG Build#1315
 */
package com.gigaspaces.time;

/**
 * {@link ITimeProvider} implementation of the underlying OS system or wall-clock time. Note that
 * while the unit of time of the return value is a millisecond, the granularity of the value depends
 * on the underlying operating system and may be larger. <p> For example, many operating systems
 * measure time in units of tens of milliseconds. See the description of the class Date for a
 * discussion of slight discrepancies that may arise between "computer time" and coordinated
 * universal time (UTC).
 */
public final class AbsoluteTime implements ITimeProvider {
    /**
     * Relies on the underlying implementation of {@link System#currentTimeMillis()}.
     *
     * @return the difference, measured in milliseconds, between the current time and midnight,
     * January 1, 1970 UTC.
     */
    public long timeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public boolean isRelative() {
        return false;
    }
}
