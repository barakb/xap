/*
 * @(#)MockLogFile.java   13/09/2010
 *
 * Copyright 2010 GigaSpaces Technologies Inc.
 */

package com.sun.jini.mahalo.log;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class MockLogFile
        implements Log {
    public void recover(LogRecovery client) throws LogException {
        throw new UnsupportedOperationException();
    }

    public long cookie() {
        return 0;
    }

    public void write(LogRecord rec) throws LogException {
    }

    public void invalidate() throws LogException {
    }
}
