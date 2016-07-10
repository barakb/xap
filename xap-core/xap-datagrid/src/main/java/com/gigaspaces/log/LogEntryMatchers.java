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


package com.gigaspaces.log;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * A set of static construction methods for {@link com.gigaspaces.log.LogEntryMatcher}s.
 *
 * @author kimchy
 */
public abstract class LogEntryMatchers {

    public static final boolean INCLUSIVE = false;
    public static final boolean EXCLUSIVE = true;

    public static final String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    // AFTER TIME

    public static AfterTimeLogEntryMatcher afterTime(long timestamp) {
        return new AfterTimeLogEntryMatcher(timestamp);
    }

    public static AfterTimeLogEntryMatcher afterTime(long timestamp, boolean inclusive) {
        return new AfterTimeLogEntryMatcher(timestamp, inclusive);
    }

    public static AfterTimeLogEntryMatcher afterTime(long timestamp, boolean inclusive, LogEntryMatcher matcher) {
        return new AfterTimeLogEntryMatcher(timestamp, inclusive, matcher);
    }

    public static AfterTimeLogEntryMatcher afterTime(long timestamp, LogEntryMatcher matcher) {
        return new AfterTimeLogEntryMatcher(timestamp, matcher);
    }

    public static AfterTimeLogEntryMatcher afterTime(String time) throws ParseException {
        return new AfterTimeLogEntryMatcher(time);
    }

    public static AfterTimeLogEntryMatcher afterTime(String time, boolean inclusive) throws ParseException {
        return new AfterTimeLogEntryMatcher(time, inclusive);
    }

    public static AfterTimeLogEntryMatcher afterTime(String time, boolean inclusive, LogEntryMatcher matcher) throws ParseException {
        return new AfterTimeLogEntryMatcher(time, inclusive, matcher);
    }

    public static AfterTimeLogEntryMatcher afterTime(String time, LogEntryMatcher matcher) throws ParseException {
        return new AfterTimeLogEntryMatcher(time, matcher);
    }

    public static AfterTimeLogEntryMatcher afterTime(String time, String format) throws ParseException {
        return new AfterTimeLogEntryMatcher(time, format);
    }

    public static AfterTimeLogEntryMatcher afterTime(String time, String format, boolean inclusive) throws ParseException {
        return new AfterTimeLogEntryMatcher(time, format, inclusive);
    }

    public static AfterTimeLogEntryMatcher afterTime(String time, String format, boolean inclusive, LogEntryMatcher matcher) throws ParseException {
        return new AfterTimeLogEntryMatcher(time, format, inclusive, matcher);
    }

    public static AfterTimeLogEntryMatcher afterTime(String time, String format, LogEntryMatcher matcher) throws ParseException {
        return new AfterTimeLogEntryMatcher(time, format, matcher);
    }

    // BEFORE TIME

    public static BeforeTimeLogEntryMatcher beforeTime(long timestamp) {
        return new BeforeTimeLogEntryMatcher(timestamp);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(long timestamp, boolean inclusive) {
        return new BeforeTimeLogEntryMatcher(timestamp, inclusive);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(long timestamp, boolean inclusive, LogEntryMatcher matcher) {
        return new BeforeTimeLogEntryMatcher(timestamp, inclusive, matcher);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(long timestamp, LogEntryMatcher matcher) {
        return new BeforeTimeLogEntryMatcher(timestamp, matcher);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(String time) throws ParseException {
        return new BeforeTimeLogEntryMatcher(time);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(String time, boolean inclusive) throws ParseException {
        return new BeforeTimeLogEntryMatcher(time, inclusive);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(String time, boolean inclusive, LogEntryMatcher matcher) throws ParseException {
        return new BeforeTimeLogEntryMatcher(time, inclusive, matcher);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(String time, LogEntryMatcher matcher) throws ParseException {
        return new BeforeTimeLogEntryMatcher(time, matcher);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(String time, String format) throws ParseException {
        return new BeforeTimeLogEntryMatcher(time, format);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(String time, String format, boolean inclusive) throws ParseException {
        return new BeforeTimeLogEntryMatcher(time, format, inclusive);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(String time, String format, boolean inclusive, LogEntryMatcher matcher) throws ParseException {
        return new BeforeTimeLogEntryMatcher(time, format, inclusive, matcher);
    }

    public static BeforeTimeLogEntryMatcher beforeTime(String time, String format, LogEntryMatcher matcher) throws ParseException {
        return new BeforeTimeLogEntryMatcher(time, format, matcher);
    }

    // After Entry

    public static AfterEntryLogEntryMatcher afterEntry(LogEntries logEntries, LogEntry logEntry) {
        return new AfterEntryLogEntryMatcher(logEntries, logEntry);
    }

    public static AfterEntryLogEntryMatcher afterEntry(LogEntries logEntries, LogEntry logEntry, boolean inclusive) {
        return new AfterEntryLogEntryMatcher(logEntries, logEntry, inclusive);
    }

    public static AfterEntryLogEntryMatcher afterEntry(LogEntries logEntries, LogEntry logEntry, LogEntryMatcher matcher) {
        return new AfterEntryLogEntryMatcher(logEntries, logEntry, matcher);
    }

    public static AfterEntryLogEntryMatcher afterEntry(LogEntries logEntries, LogEntry logEntry, boolean inclusive, LogEntryMatcher matcher) {
        return new AfterEntryLogEntryMatcher(logEntries, logEntry, inclusive, matcher);
    }

    // Before Entry

    public static BeforeEntryLogEntryMatcher beforeEntry(LogEntries logEntries, LogEntry logEntry) {
        return new BeforeEntryLogEntryMatcher(logEntries, logEntry);
    }

    public static BeforeEntryLogEntryMatcher beforeEntry(LogEntries logEntries, LogEntry logEntry, boolean inclusive) {
        return new BeforeEntryLogEntryMatcher(logEntries, logEntry, inclusive);
    }

    public static BeforeEntryLogEntryMatcher beforeEntry(LogEntries logEntries, LogEntry logEntry, LogEntryMatcher matcher) {
        return new BeforeEntryLogEntryMatcher(logEntries, logEntry, matcher);
    }

    public static BeforeEntryLogEntryMatcher beforeEntry(LogEntries logEntries, LogEntry logEntry, boolean inclusive, LogEntryMatcher matcher) {
        return new BeforeEntryLogEntryMatcher(logEntries, logEntry, inclusive, matcher);
    }

    // simple handlers

    public static LastNLogEntryMatcher lastN(int lastN) {
        return new LastNLogEntryMatcher(lastN);
    }

    public static LastNLogEntryMatcher lastN(int lastN, LogEntryMatcher matcher) {
        return new LastNLogEntryMatcher(lastN, matcher);
    }

    public static RegexLogEntryMatcher regex(String regex) {
        return new RegexLogEntryMatcher(regex);
    }

    public static RegexLogEntryMatcher regex(String regex, LogEntryMatcher matcher) {
        return new RegexLogEntryMatcher(regex, matcher);
    }

    public static ContainsStringLogEntryMatcher containsString(String str) {
        return new ContainsStringLogEntryMatcher(str);
    }

    public static ContainsStringLogEntryMatcher containsString(String str, LogEntryMatcher matcher) {
        return new ContainsStringLogEntryMatcher(str, matcher);
    }

    public static FirstFileLogEntryMatcher firstFile() {
        return new FirstFileLogEntryMatcher();
    }

    public static FirstFileLogEntryMatcher firstFile(LogEntryMatcher matcher) {
        return new FirstFileLogEntryMatcher(matcher);
    }

    // streams

    public static ContinuousLogEntryMatcher continuous(LogEntryMatcher initialMatcher) {
        return new ContinuousLogEntryMatcher(initialMatcher, null);
    }

    public static ContinuousLogEntryMatcher continuous(LogEntryMatcher initialMatcher, LogEntryMatcher continousMatcher) {
        return new ContinuousLogEntryMatcher(initialMatcher, continousMatcher);
    }

    public static ForwardChunkLogEntryMatcher forwardChunk(LogEntryMatcher matcher) {
        return new ForwardChunkLogEntryMatcher(matcher);
    }

    public static ReverseLogEntryMatcher reverse(LogEntryMatcher matcher) {
        return new ReverseLogEntryMatcher(matcher);
    }

    // content handlers

    public static AllLogEntryMatcher all() {
        return new AllLogEntryMatcher();
    }

    public static SizeLogEntryMatcher size(int size) {
        return new SizeLogEntryMatcher(size);
    }

    public static NoneLogEntryMatcher none() {
        return new NoneLogEntryMatcher();
    }

    static SimpleDateFormat createDateFormat(String format) {
        SimpleDateFormat df = new SimpleDateFormat(format);
        df.setLenient(true);
        return df;
    }

    private LogEntryMatchers() {

    }
}
