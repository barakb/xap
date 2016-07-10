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

package com.gigaspaces.metrics.influxdb;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.gigaspaces.internal.utils.StringUtils.NEW_LINE;

/**
 * @author Niv Ingberg
 * @since 10.2.1
 */
public abstract class InfluxDBDispatcher implements Closeable {

    private static final long THRESHOLD = 10;
    protected final Logger logger = Logger.getLogger(this.getClass().getName());
    private final AtomicLong failures = new AtomicLong();

    public void send(String data) {
        if (logger.isLoggable(Level.FINEST))
            logger.finest("Sending the following data: " + NEW_LINE + data);
        try {
            doSend(data);
            final long consecutive = failures.get();
            if (consecutive != 0)
                logger.info("Report was sent successfully after " + consecutive + " consecutive failures");
            failures.set(0);
        } catch (IOException e) {
            final long consecutive = failures.incrementAndGet();
            if (consecutive <= THRESHOLD && logger.isLoggable(Level.WARNING)) {
                String message;
                if (consecutive < THRESHOLD)
                    message = "Failed to send report (" + consecutive + " consecutive failures)" + NEW_LINE + data;
                else
                    message = "Failed to send report (" + consecutive + " consecutive failures - additional failures will be logged as FINEST until the problem is resolved)" + NEW_LINE + data;
                logger.log(Level.WARNING, message, e);
            } else {
                if (logger.isLoggable(Level.FINEST))
                    logger.log(Level.FINEST, "Failed to send report ( + " + consecutive + " consecutive failures)" + NEW_LINE + data, e);
            }
        }
    }

    protected abstract void doSend(String content) throws IOException;

    @Override
    public void close() {
    }
}
