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
 * Title:		 RelativeTime.java
 * Description: High Precision timer implementation of ITimeProvider interface.
 * Company:		 GigaSpaces Technologies
 * 
 * @author		 Moran Avigdor
 * @version		 1.0 12/12/2005
 * @since		 5.0EAG Build#1315
 */
package com.gigaspaces.time;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * {@linkplain ITimeProvider ITimeProvider} implementation specifying a precise timer, which is not
 * influenced by motions of the wall-clock time.
 *
 * This class contains methods related to millisecond-precision timing, particularly via the {@link
 * #timeMillis()} method. To measure time accurately, this method uses <code>java.sun.Perf</code>
 * (since JDK1.4.2). The resolution of the timer is machine dependent. <p>
 * <i><b>Limitations:</b></i> <li>sun.misc.Perf is not part of the core distribution, so there is no
 * guarantee it will be present in future releases. More likely wont be present in JVMs distributed
 * by other manufacturers.</li> <li>Relative Time is currently limited to Windows OS. <li>Relative
 * time is currently limited to in-memory space.</li> </p>
 */
public final class RelativeTime implements ITimeProvider {
    final sun.misc.Perf _perf;
    final long TICKS_PER_SECOND;
    final static long TO_MILLISECONDS = 1000;

    /**
     * Returns a RelativeTime instance based on sun.misc.Perf precision.
     *
     * @throws RuntimeException if failed to access to sun.misc.perf
     */
    public RelativeTime() {
        _perf =
                AccessController.doPrivileged(new PrivilegedAction<sun.misc.Perf>() {
                    public sun.misc.Perf run() {
                        return sun.misc.Perf.getPerf();
                    }
                });

        /*
         * Verify limitations as specified in the class javadoc.
         */
        if (_perf == null)
            throw new RuntimeException("Limitation - unable to load sun.misc.Perf provider; " +
                    "\n\t The undocumented sun.misc.Perf class provides high resolution access " +
                    "to the system clock. @since Sun JDK1.4.2\n");

        if (!System.getProperty("file.separator").equals("\\"))
            throw new RuntimeException("Limitation - Relative Time is currently limited to Windows OS.");

        TICKS_PER_SECOND = _perf.highResFrequency();
    }

    /**
     * Returns the current value of the most precise available system timer, in milliseconds. This
     * method is used to measure elapsed time and is not related to any other notion of system or
     * wall-clock time.
     *
     * @return The current value of the system timer, in milliseconds.
     */
    public long timeMillis() {
        return ((_perf.highResCounter() * TO_MILLISECONDS) / TICKS_PER_SECOND);
    }

    @Override
    public boolean isRelative() {
        return true;
    }
}
