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
 * Title:		 SystemTime.java
 * Description: A static system time wrapper
 * Company:		 GigaSpaces Technologies
 * 
 * @author		 Moran Avigdor
 * @version		 1.0 12/12/2005
 * @since		 5.0EAG Build#1315
 */
package com.gigaspaces.time;

import com.gigaspaces.start.SystemInfo;

/**
 * A static system time wrapper, which will call the underlying <i>{@link ITimeProvider}</i>
 * implementation. <p> <b><u>usage:</u></b> <p> <code> long time = SystemTime.timeMillis(); </code>
 * <p> <i>{@link AbsoluteTime}</i> is the default implementation of <code>ITimeProvider</code>.
 * Custom time implementations can be provided by use of a system property. The value of the
 * property should name a class implementing <code>ITimeProvider</code> interface.
 * <pre>
 * <b>e.g.</b> for applying a <i>"relative time"</i> provider, you can either:
 *
 * 1. Set via System property: (recommended option)
 * <code>
 * 	-Dcom.gs.time-provider=com.j_spaces.kernel.time.RelativeTime
 * </code>
 * 2. Set via container xml-tag value:
 * <code>
 * 	&lt;time-provider&gt;com.j_spaces.kernel.time.RelativeTime&lt;/time-provider&gt;
 * </code>
 * </pre>
 * <p> All known implementations, can be queried as follows: <li><tt>SystemTime.isAbsoluteTime();</tt></li>
 * <li><tt>SystemTime.isRelativeTime();<tt></li>
 */
public final class SystemTime {
    /**
     * Returns the time in milliseconds specified by the underlying ITimerProvider implementation.
     *
     * Note that while the unit of time of the return value is a millisecond, the granularity of the
     * value depends on the time-provider implementation and may be larger.
     *
     * @return the difference, measured in milliseconds, between the system time and lifetime of the
     * JVM.
     */
    public static long timeMillis() {
        return SystemInfo.singleton().timeProvider().timeMillis();
    }
}
