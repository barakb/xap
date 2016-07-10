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


package com.gigaspaces.metrics.reporters;

import com.gigaspaces.metrics.MetricGroupSnapshot;
import com.gigaspaces.metrics.MetricRegistrySnapshot;
import com.gigaspaces.metrics.MetricReporter;
import com.gigaspaces.metrics.MetricTagsSnapshot;

import java.io.PrintStream;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 10.1
 */

public class ConsoleReporter extends MetricReporter {

    private static final int CONSOLE_WIDTH = 80;
    private static final String NEWLINE = System.getProperty("line.separator");

    private final PrintStream output;
    private final DateFormat dateFormat;
    private final StringBuilder buffer = new StringBuilder();

    public ConsoleReporter(ConsoleReporterFactory factory) {
        super(factory);
        this.output = factory.getOutput();
        this.dateFormat = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM, factory.getLocale());
        this.dateFormat.setTimeZone(factory.getTimeZone());
    }

    public void report(List<MetricRegistrySnapshot> snapshots) {
        super.report(snapshots);
        output.print(buffer.toString());
        output.flush();
        buffer.setLength(0);
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot) {
        printWithBanner(dateFormat.format(new Date(snapshot.getTimestamp())), '=');
        super.report(snapshot);
        buffer.append(NEWLINE);
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, MetricGroupSnapshot group) {
        buffer.append("\tTags: ").append(tags).append(NEWLINE);
        super.report(snapshot, tags, group);
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, String key, Object value) {
        buffer.append("\t\t").append(key).append(" => ").append(value).append(NEWLINE);
    }

    private void printWithBanner(String s, char c) {
        buffer.append(s);
        buffer.append(' ');
        for (int i = 0; i < (CONSOLE_WIDTH - s.length() - 1); i++)
            buffer.append(c);
        buffer.append(NEWLINE);
    }
}
