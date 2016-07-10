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

package com.j_spaces.core.filters;

import com.gigaspaces.internal.utils.ClassUtils;

import java.io.Serializable;

/**
 * A filter provider acts as a wrapper around an {@link com.j_spaces.core.filters.ISpaceFilter}
 * allowing to provide a an already instantiated filter using {@link
 * com.j_spaces.core.client.SpaceFinder} with custom {@link java.util.Properties}.
 *
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class FilterProvider implements Serializable {
    private static final long serialVersionUID = 4246025503339320719L;

    private String name;

    private ISpaceFilter filter;

    private boolean activeWhenBackup = true;

    private boolean enabled = true;

    private boolean securityFilter = false;

    private boolean shutdownSpaceOnInitFailure = false;

    private int priority = 0;

    private int[] opCodes = new int[0];

    private String filterParam;

    private boolean passFilterEntry = true;

    /**
     *
     */
    public FilterProvider() {
        super();

    }

    public FilterProvider(ISpaceFilter filter) {
        this(null, filter);
    }

    /**
     * Constructs a new filter provider using the provided filter and the filter name.
     *
     * @param filterName The filter name
     * @param filter     The filter
     */
    public FilterProvider(String filterName, ISpaceFilter filter) {
        this.name = filterName;
        if (this.name == null) {
            this.name = ClassUtils.getShortName(filter.getClass());
        }
        this.filter = filter;
        if (filter == null) {
            throw new IllegalArgumentException("filter must not be null");
        }
    }

    /**
     * Returns the name of the filter.
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the filter object.
     */
    public ISpaceFilter getFilter() {
        return filter;
    }

    /**
     * Returns <code>true</code> if the filter should be active when the space is in backup mode.
     * Default to <code>true</code>.
     */
    public boolean isActiveWhenBackup() {
        return activeWhenBackup;
    }

    /**
     * <code>true</code> if the filter should be active when the space is in backup mode. Default to
     * <code>true</code>.
     */
    public void setActiveWhenBackup(boolean activeWhenBackup) {
        this.activeWhenBackup = activeWhenBackup;
    }

    /**
     * Returns <code>true</code> if the filter is enabled. Defaults to <code>true</code>.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * <code>true</code> if the filter is enabled. Defaults to <code>true</code>.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns <code>true</code> if the filter is a security filter. Defaults to
     * <code>false</code>.
     */
    public boolean isSecurityFilter() {
        return securityFilter;
    }

    /**
     * <code>true</code> if the filter is a security filter. Defaults to <code>false</code>.
     */
    public void setSecurityFilter(boolean securityFilter) {
        this.securityFilter = securityFilter;
    }

    /**
     * Returns <code>true</code> if the space should shutdown on filter init failure. Defaults to
     * <code>false</code>.
     */
    public boolean isShutdownSpaceOnInitFailure() {
        return shutdownSpaceOnInitFailure;
    }

    /**
     * <code>true</code> if the space should shutdown on filter init failure. Defaults to
     * <code>false</code>.
     */
    public void setShutdownSpaceOnInitFailure(boolean shutdownSpaceOnInitFailure) {
        this.shutdownSpaceOnInitFailure = shutdownSpaceOnInitFailure;
    }

    /**
     * Returns the list of operation codes to be filtered.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes
     */
    public int[] getOpCodes() {
        return opCodes;
    }

    /**
     * Sets the list of operation codes to be filtered.
     *
     * @see com.j_spaces.core.filters.FilterOperationCodes
     */
    public void setOpCodes(int... opCodes) {
        this.opCodes = opCodes;
    }

    /**
     * Returns the priority of the filter. Defaults to <code>0</code>.
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Sets the priority of the filter. Defaults to <code>0</code>.
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    public String getFilterParam() {
        return filterParam;
    }

    public void setFilterParam(String filterParam) {
        this.filterParam = filterParam;
    }

    public void setPassFilterEntry(boolean passFilterEntry) {
        this.passFilterEntry = passFilterEntry;
    }

    public boolean isPassFilterEntry() {
        return passFilterEntry;
    }
}
