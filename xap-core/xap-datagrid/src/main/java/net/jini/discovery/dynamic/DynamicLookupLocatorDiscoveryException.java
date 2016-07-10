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
@com.gigaspaces.api.InternalApi
public class DynamicLookupLocatorDiscoveryException
        extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DynamicLookupLocatorDiscoveryException(Exception e) {
        super(e);
    }

    public DynamicLookupLocatorDiscoveryException(String msg, Exception e) {
        super(msg, e);
    }

}
