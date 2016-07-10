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


package com.gigaspaces.security.authorities;

import com.gigaspaces.security.Authority;

/**
 * Defines an Authority for monitoring a service, with the specified privilege. <p> <b>Note: These
 * privileges are not enforced by the API and should be properly handled by administrative tooling
 * (UI/CLI/Application).</b> <p> The <code>MonitorAuthority</code>s' {@link
 * Authority#getAuthority()} String representation format:
 * <pre>
 * <code>MonitorPrivilege privilege-value</code>
 *
 * Where:
 * privilege-value = MONITOR_JVM | MONITOR_PU
 *
 * The privileges represent the following grid operations:
 * MONITOR_JVM   - JVM statistics
 * MONITOR_PU    - processing unit statistics (classes/templates/transactions/connections/statistics,
 * etc.)
 * </pre>
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class MonitorAuthority implements InternalAuthority {

    /**
     * Defines monitoring privileges
     */
    public enum MonitorPrivilege implements Privilege {
        /**
         * JVM statistics
         */
        MONITOR_JVM,
        /**
         * Processing unit statistics (classes/templates/transactions/connections/statistics, etc.)
         */
        MONITOR_PU;

        @Override
        public String toString() {
            switch (this) {
                case MONITOR_JVM:
                    return "Monitor JVM";
                case MONITOR_PU:
                    return "Monitor PU";
                default:
                    return super.toString();
            }
        }
    }

    private static final long serialVersionUID = 1L;
    private final MonitorPrivilege monitorPrivilege;

    public MonitorAuthority(MonitorPrivilege monitorPrivilege) {
        this.monitorPrivilege = monitorPrivilege;
    }

    public static MonitorAuthority valueOf(String authority) {
        if (authority == null) {
            throw new IllegalArgumentException("Illegal Authority format: null");
        }

        String[] split = authority.split(Constants.DELIM);
        if (split.length < 2) {
            throw new IllegalArgumentException("Illegal Authority format: " + authority);
        }

        if (!MonitorPrivilege.class.getSimpleName().equals(split[Constants.PRIVILEGE_NAME_POS])) {
            throw new IllegalArgumentException("Illegal Privilege name in: " + authority);
        }

        MonitorPrivilege monitorPrivilege = MonitorPrivilege.valueOf(split[Constants.PRIVILEGE_VAL_POS]);
        return new MonitorAuthority(monitorPrivilege);
    }

    /*
     * @see com.gigaspaces.security.Authority#getAuthority()
     */
    public String getAuthority() {
        return monitorPrivilege.getClass().getSimpleName() + Constants.DELIM + monitorPrivilege.name();
    }

    /*
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return getAuthority();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((getAuthority() == null) ? 0 : getAuthority().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MonitorAuthority other = (MonitorAuthority) obj;
        if (getAuthority() == null) {
            if (other.getAuthority() != null)
                return false;
        } else if (!getAuthority().equals(other.getAuthority()))
            return false;
        return true;
    }

    /*
     * @see com.gigaspaces.security.authorities.InternalAuthority#getMappingKey()
     */
    public Privilege getPrivilege() {
        return monitorPrivilege;
    }
}
