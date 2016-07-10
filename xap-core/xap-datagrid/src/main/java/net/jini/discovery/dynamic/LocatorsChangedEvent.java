/*******************************************************************************
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
package net.jini.discovery.dynamic;

import net.jini.core.discovery.LookupLocator;

import java.util.EventObject;

/**
 * @author Dan Kilman
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class LocatorsChangedEvent extends EventObject {
    private static final long serialVersionUID = 1L;

    private final LookupLocator[] _locators;

    public LocatorsChangedEvent(Object source, LookupLocator[] locators) {
        super(source);
        _locators = locators;
    }

    public LookupLocator[] getLocators() {
        return _locators;
    }
}
