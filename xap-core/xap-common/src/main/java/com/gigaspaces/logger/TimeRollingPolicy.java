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

package com.gigaspaces.logger;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * A rolling policy based on time. If the specified time has elapsed, {@link #needsRollover()}
 * returns <code>true</code>. The time policy can be set to either one of: daily, weekly, monthly or
 * yearly.
 *
 * @author Moran Avigdor
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class TimeRollingPolicy {

    private enum TimeBoundry {
        DAILY, WEEKLY, MONTHLY, YEARLY;
    }

    private final TimeBoundry timeBoundry;
    private Calendar nextRollover;

    TimeRollingPolicy(String policy) {
        String boundry = policy.trim().toUpperCase();
        timeBoundry = TimeBoundry.valueOf(boundry);
    }

    /**
     * Sets the timestamp of the next rollover event.
     */
    void setTimestamp() {
        Calendar cal = GregorianCalendar.getInstance();
        switch (timeBoundry) {
            case DAILY:
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                cal.add(Calendar.DATE, 1);
                break;
            case WEEKLY:
                cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                cal.add(Calendar.WEEK_OF_YEAR, 1);
                break;
            case MONTHLY:
                cal.set(Calendar.DATE, 1);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                cal.add(Calendar.MONTH, 1);
                break;
            case YEARLY:
                cal.set(Calendar.DATE, 1);
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
                cal.set(Calendar.MONTH, 0);
                cal.add(Calendar.YEAR, 1);
                break;
        }
        this.nextRollover = cal;
    }

    /**
     * If it is time to rollover, calculate next rollover event and return true.
     *
     * @return <code>true</code> if the time has elapsed; <code>false</code> otherwise.
     */
    boolean needsRollover() {
        Calendar now = GregorianCalendar.getInstance();
        if (now.compareTo(nextRollover) > 0) {
            setTimestamp();
            return true;
        } else {
            return false;
        }
    }
}
