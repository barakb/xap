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

package org.openspaces.archive;

import com.gigaspaces.time.SystemTime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.events.EventExceptionHandler;
import org.openspaces.events.ListenerExecutionFailedException;
import org.springframework.transaction.TransactionStatus;

import java.util.concurrent.TimeUnit;

/**
 * @author Itai Frenkel
 *
 *         Logs exceptions from the {@link ArchivePollingContainer} Filters last exception so it
 *         won't be repeatedly logged more than once per minute.
 */
public class DefaultArchivePollingContainerExceptionHandler implements EventExceptionHandler<Object> {

    private static final long DEFAULT_TIME_WINDOW_MILLIS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

    private final Log logger = LogFactory.getLog(this.getClass());

    private Class<? extends Throwable> lastExceptionClass;
    private String lastExceptionMessage;
    private long lastExceptionTimestamp;

    private final long timeWindowMillis;

    public DefaultArchivePollingContainerExceptionHandler() {
        this(DEFAULT_TIME_WINDOW_MILLIS);
    }

    public DefaultArchivePollingContainerExceptionHandler(long timeWindowsMillis) {
        this.timeWindowMillis = timeWindowsMillis;
    }

    /**
     * This is the default handler if {@link #setExceptionHandler(EventExceptionHandler)} was not
     * called. Does nothing in case of a successful archive operation
     */
    @Override
    public void onSuccess(Object data, GigaSpace gigaSpace, TransactionStatus txStatus, Object source)
            throws RuntimeException {
        //ignore
    }

    /**
     * This is the default handler if {@link #setExceptionHandler(EventExceptionHandler)} was not
     * called. Logs the exception if it has not occured in the last minute.
     */
    @Override
    public void onException(ListenerExecutionFailedException exception, Object data, GigaSpace gigaSpace,
                            TransactionStatus txStatus, Object source) throws RuntimeException {

        long now = SystemTime.timeMillis();
        if (logger.isWarnEnabled() && shouldLog(exception, now)) {
            logger.warn("Failed to archive data", exception);
            storeLastException(exception, now);
        }

        throw exception;
    }

    public void storeLastException(ListenerExecutionFailedException exception, long now) {
        Throwable cause = exception.getCause();
        this.lastExceptionClass = (cause == null) ? null : cause.getClass();
        this.lastExceptionTimestamp = now;
        this.lastExceptionMessage = (cause == null) ? null : cause.getMessage();
    }

    public boolean shouldLog(ListenerExecutionFailedException exception, long now) {
        final Throwable cause = exception.getCause();
        return !isSameExecptionType(cause) ||
                !isSameMessage(cause) ||
                !isSameTime(now);

    }

    private boolean isSameMessage(Throwable cause) {
        return (cause == null && lastExceptionMessage == null) ||
                (cause != null && cause.getMessage() == null && lastExceptionMessage == null) ||
                (cause != null && lastExceptionMessage != null && lastExceptionMessage.equals(cause.getMessage()));
    }

    private boolean isSameExecptionType(Throwable cause) {
        return (cause == null && lastExceptionClass == null) ||
                (cause != null && lastExceptionClass != null && lastExceptionClass.equals(cause.getClass()));
    }

    private boolean isSameTime(long now) {
        return now - lastExceptionTimestamp < getTimeWindowMillis();
    }

    public long getTimeWindowMillis() {
        return timeWindowMillis;
    }
}
