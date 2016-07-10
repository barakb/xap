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

import java.util.ResourceBundle;
import java.util.logging.Filter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A logger that does not pass through volatile on isLoggable call in order to reduce number of
 * memory barriers
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class UnsafeLevelLogger
        extends Logger {
    private static final int offValue = Level.OFF.intValue();
    private final Logger _logger;
    private int _logLevel;
    private boolean _levelOverriden;

    protected UnsafeLevelLogger(String name, String resourceBundleName) {
        super(name, resourceBundleName);
        throw new UnsupportedOperationException();
    }

    public UnsafeLevelLogger(String name, Logger logger) {
        super(name, null);
        _logger = logger;
    }

    public static UnsafeLevelLogger getLogger(String name) {
        return new UnsafeLevelLogger(name, Logger.getLogger(name));
    }

    @Override
    public void setLevel(Level newLevel) throws SecurityException {
        synchronized (this) {
            _levelOverriden = true;
            _logLevel = newLevel.intValue();
        }
        _logger.setLevel(newLevel);
    }

    @Override
    public boolean isLoggable(Level level) {
        //The optimization is only relevant if set level was explicitly called, we do call it
        //explicit due to our gs_logging file. if it is not called we cannot know the actual log level
        //due to internal logger logic
        if (!_levelOverriden)
            return _logger.isLoggable(level);

        if (level.intValue() < _logLevel || _logLevel == offValue) {
            return false;
        }
        return true;
    }

    @Override
    public void log(LogRecord record) {
        _logger.log(record);
    }

    @Override
    public Filter getFilter() {
        return _logger.getFilter();
    }

    @Override
    public synchronized Handler[] getHandlers() {
        return _logger.getHandlers();
    }

    @Override
    public Level getLevel() {
        return _logger.getLevel();
    }

    @Override
    public Logger getParent() {
        return _logger.getParent();
    }

    @Override
    public boolean getUseParentHandlers() {
        return _logger.getUseParentHandlers();
    }

    @Override
    public String getName() {
        return _logger.getName();
    }

    @Override
    public ResourceBundle getResourceBundle() {
        return _logger.getResourceBundle();
    }

    @Override
    public String getResourceBundleName() {
        return _logger.getResourceBundleName();
    }

    @Override
    public int hashCode() {
        return _logger.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return _logger.equals(obj);
    }

    @Override
    public void removeHandler(Handler handler)
            throws SecurityException {
        _logger.removeHandler(handler);
    }

    @Override
    public void setFilter(Filter newFilter) throws SecurityException {
        _logger.setFilter(newFilter);
    }

    @Override
    public void setParent(Logger parent) {
        _logger.setParent(parent);
    }

    @Override
    public void setUseParentHandlers(boolean useParentHandlers) {
        _logger.setUseParentHandlers(useParentHandlers);
    }


}
