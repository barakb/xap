/*******************************************************************************
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
package net.jini.discovery.dynamic;

/**
 * @author Dan Kilman
 * @since 9.1
 */

/**
 * Listener for locators change events. 
 * see {@link DynamicLookupLocatorDiscovery#addLookupLocatorsChangeListener(LookupLocatorsChangeListener)}
 */
public interface LookupLocatorsChangeListener {
    void locatorsChanged(LocatorsChangedEvent e);
}
