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

/**
 * A marker interface for a specific {@link com.gigaspaces.log.LogEntryMatcher} marking it as needed
 * to have extra processing step when it is received on the client side.
 *
 * <p>Will be called automatically when used with the Admin API.
 *
 * @author kimchy
 */
public interface ClientLogEntryMatcherCallback extends LogEntryMatcher {

    /**
     * Process the log entries and use the resulted entries.
     */
    LogEntries clientSideProcess(LogEntries entries);
}
