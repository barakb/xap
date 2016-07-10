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


package com.j_spaces.sadapter.cache;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.EntryImpl;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.IJSpace;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;

/**
 * CacheEntry (implements {@link IGSEntry}) which is passed between {@link CacheAdapter} and {@link
 * CacheStorage}.
 *
 * @author moran
 * @version 1.0
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class CacheEntry extends EntryImpl {
    private static final long serialVersionUID = -7641652996134448737L;

    private String _className;

    /**
     * Constructs CacheEntry from IEntryHolder and TypeTableEntry.
     *
     * @param entryHolder underlying entry holder
     * @param typeDesc    underlying entry type information
     */
    public CacheEntry(IEntryHolder entryHolder, ITypeDesc typeDesc) {
        super(entryHolder, typeDesc);
        _className = entryHolder.getClassName();
    }

    /**
     * Default constructor
     */
    public CacheEntry() {
        super(null, null);
    }

    /*
    * @see com.j_spaces.core.filters.entry.SpaceFilterEntryImpl#getClassName()
    */
    @Override
    public String getClassName() {
        return _className;
    }

    /**
     * Set the class name representing the underlying entry.
     *
     * @param className class name representing this entry.
     */
    public void setClassName(String className) {
        _className = className;
    }

    @Override
    public Entry getEntry(IJSpace space) throws UnusableEntryException {
        throw new UnsupportedOperationException();
    }

    @Override
    public java.util.Map.Entry getMapEntry() {
        return null;
    }

    /*
     * @return underlying class name or MapEntry representation.
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return toString(this);
    }

    /**
     * Returns a String representation of an IGSEntry.
     *
     * @param entry IGSEntry to represent.
     * @return String representation of this IGSEntry.
     */
    public static String toString(IGSEntry entry) {
        if (entry.getMapEntry() == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("Entry<");
            sb.append(entry.getClassName());
            sb.append(", UID: " + entry.getUID());

            String[] fieldNames = entry.getFieldsNames();
            if (fieldNames != null) {
                sb.append(entry.isTransient() ? ", *transient*" : "");
                sb.append(", Fields: ");

                for (int i = 0; i < fieldNames.length; ++i) {
                    Object value = entry.getFieldValue(i);
                    sb.append(fieldNames[i]);
                    sb.append(": ");
                    sb.append(value);
                    sb.append(", ");
                }
            }
            sb.append(">");
            return sb.toString();
        }
        return entry.getMapEntry().toString();
    }
}
