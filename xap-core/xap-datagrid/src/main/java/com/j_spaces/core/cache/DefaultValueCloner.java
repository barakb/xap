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


package com.j_spaces.core.cache;

import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.internal.utils.ClassLoaderThreadLocal;
import com.j_spaces.core.client.FieldValueCloningErrorException;

import java.io.ObjectStreamConstants;


/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.04
 */

/*
 * clone (using clone() or marshaling a value object
 */
@com.gigaspaces.api.InternalApi
public class DefaultValueCloner
        implements IValueCloner {
    final private static byte[] _resetBuffer = new byte[]{ObjectStreamConstants.TC_RESET, ObjectStreamConstants.TC_NULL};

    MarshalInputStream _mis = null;
    MarshalOutputStream _mos = null;
    GSByteArrayOutputStream _os;
    GSByteArrayInputStream _is;

    private final static int INITIAL_SIZE = 64;

    private static final ClassLoaderThreadLocal<IValueCloner> _valueClonerCache = new ClassLoaderThreadLocal<IValueCloner>() {
        @Override
        protected IValueCloner initialValue() {
            return new DefaultValueCloner();
        }
    };

    public static IValueCloner get() {
        return _valueClonerCache.get();
    }

    /*
     * @see com.j_spaces.core.cache.IValueCloner#cloneValue(java.lang.Object, boolean)
     */
    @Override
    public Object cloneValue(Object originalValue, boolean isCloneable, Class<?> clzz, String uid, String entryClassName) {
        if (isCloneable && clzz != null) {
            try {
                return clzz.getMethod("clone", new Class[0]).invoke(originalValue, new Object[0]);
            } catch (Exception ex) {
            }
        }
        try {
            if (_mis == null) {//create
                _os = new GSByteArrayOutputStream(INITIAL_SIZE);
                _is = new GSByteArrayInputStream(_os.getBuffer());
                _mos = new MarshalOutputStream(_os);
                _mis = new MarshalInputStream(_is);

            }

            _mos.writeUnshared(originalValue);
            _is.setBuffer(_os.getBuffer(), _os.getCount());
            Object res = _mis.readUnshared();

            return res;
        } catch (Exception ex) {
            if (clzz == null)
                clzz = originalValue.getClass();
            throw new FieldValueCloningErrorException(uid, entryClassName, clzz.getName(), originalValue, ex);
        } finally {
            reset();
        }
    }

    public void reset() {
        try {
            if (_mos != null) {
                _is.reset();
                _os.reset();
                _mos.reset();
                //this is the only way to do reset on ObjetInputStream:
                // add reset flag and let the ObjectInputStream to read it 
                // so all the handles in the ObjectInputStream will be cleared
                byte[] buffer = _is.getBuffer();
                _is.setBuffer(_resetBuffer);
                _mis.readObject();
                _is.setBuffer(buffer);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

}
