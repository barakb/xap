/*
 * @(#)MockLogManager.java   13/09/2010
 *
 * Copyright 2010 GigaSpaces Technologies Inc.
 */

package com.sun.jini.mahalo.log;


import net.jini.admin.Administrable;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.0
 */

/**
 * mock log manager for transient mahalo
 */
@com.gigaspaces.api.InternalApi
public class MockLogManager
        implements LogManager, FileModes, Administrable, MultiLogManagerAdmin {

    public ClientLog logFor(long cookie) throws LogException {
        return new MockLogFile();
    }

    // javadoc inherited from supertype
    private void release(long cookie) {
    }

    /**
     * Consumes the log file and re-constructs a system's state.
     */
    public void recover() throws LogException {

    }


    /**
     * Retrieves the administration interface for the <code>MultiLogManager</code>
     */
    public Object getAdmin() {
        return this;
    }


    /**
     * Clean up all <code>LogFile</code> objects on behalf of caller.
     *
     * @see com.sun.jini.admin.DestroyAdmin
     * @see com.sun.jini.system.FileSystem
     */
    public void destroy() {
    }

}
