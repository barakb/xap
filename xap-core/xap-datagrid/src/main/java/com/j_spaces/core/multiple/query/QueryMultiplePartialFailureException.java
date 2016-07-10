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


package com.j_spaces.core.multiple.query;

import com.gigaspaces.client.ClearException;
import com.gigaspaces.client.ReadMultipleException;
import com.gigaspaces.client.TakeMultipleException;

/**
 * Thrown when read/take multiple is called and and the max limit can't be satisfied.
 *
 * @author GuyK
 * @since 6.6
 * @deprecated since 7.1. Use operation specific exception {@link ReadMultipleException},{@link
 * TakeMultipleException} or {@link ClearException}
 */

public class QueryMultiplePartialFailureException extends RuntimeException {
    private static final long serialVersionUID = -5769270833727972278L;

    private Object[] _results;
    private Throwable[] _causes;

    /**
     * Return the partial results
     *
     * @return the results that are kept by this object.
     */
    public Object[] getResults() {
        return _results;
    }

    /**
     * @param results the results to set
     */
    public void setResults(Object[] results) {
        _results = results;
    }

    /**
     * Return all the causes that caused this exception
     *
     * @return the causes of this Exception.
     */
    public Throwable[] getCauses() {
        return _causes;
    }

    protected void setCauses(Throwable[] causes) {
        this._causes = causes;
    }
}