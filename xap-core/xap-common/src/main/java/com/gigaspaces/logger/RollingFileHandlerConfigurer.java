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

/**
 * A configurer for known properties which their generated values can be overridden. <p>
 * Implementations that extend {@link RollingFileHandler} may wish to extend the call to {@link
 * RollingFileHandler#configure()} and provide calls to these static setters with user generated
 * values. <p> For example, setting the Process ID to some platform dependent implementation.
 *
 * @author Moran Avigdor
 */

public class RollingFileHandlerConfigurer {

    /**
     * Sets the {@link RollingFileHandler#HOMEDIR_PROP} property value.
     *
     * @param homedir  The home directory of this logger
     * @param override If <code>true</code> will override any setting of this property; If
     *                 <code>false</code> will not override if setting already exists for this
     *                 property.
     */
    public static void setHomedirProperty(String homedir, boolean override) {
        if (!override && RollingFileHandler.overrides.containsKey(RollingFileHandler.HOMEDIR_PROP)) {
            return;
        }
        RollingFileHandler.overrides.setProperty(RollingFileHandler.HOMEDIR_PROP, homedir);
    }

    /**
     * Sets the {@link RollingFileHandler#HOST_PROP} property value.
     *
     * @param host     The host of this VM.
     * @param override If <code>true</code> will override any setting of this property; If
     *                 <code>false</code> will not override if setting already exists for this
     *                 property.
     */
    public static void setHostProperty(String host, boolean override) {
        if (!override && RollingFileHandler.overrides.containsKey(RollingFileHandler.HOST_PROP)) {
            return;
        }
        RollingFileHandler.overrides.setProperty(RollingFileHandler.HOST_PROP, host);
    }

    /**
     * Sets the {@link RollingFileHandler#PID_PROP} property value.
     *
     * @param pid The Process ID, usually an integer.
     */
    public static void setPidProperty(String pid, boolean override) {
        if (!override && RollingFileHandler.overrides.containsKey(RollingFileHandler.PID_PROP)) {
            return;
        }
        RollingFileHandler.overrides.setProperty(RollingFileHandler.PID_PROP, pid);
    }

    /**
     * Sets the {@link RollingFileHandler#SERVICE_PROP} property value.
     *
     * @param service  The name of the service, e.g. GSM, GSC, GSA, Space.
     * @param override If <code>true</code> will override any setting of this property; If
     *                 <code>false</code> will not override if setting already exists for this
     *                 property.
     */
    public static void setServiceProperty(String service, boolean override) {
        if (!override && RollingFileHandler.overrides.containsKey(RollingFileHandler.SERVICE_PROP)) {
            return;
        }
        RollingFileHandler.overrides.setProperty(RollingFileHandler.SERVICE_PROP, service);
    }

    /**
     * Sets the {@link RollingFileHandler#DATE_PROP} property value.
     *
     * @param date     The date to be used instead of todays' date.
     * @param override If <code>true</code> will override any setting of this property; If
     *                 <code>false</code> will not override if setting already exists for this
     *                 property.
     */
    public static void setDateProperty(String date, boolean override) {
        if (!override && RollingFileHandler.overrides.containsKey(RollingFileHandler.DATE_PROP)) {
            return;
        }
        RollingFileHandler.overrides.setProperty(RollingFileHandler.DATE_PROP, date);
    }
}
