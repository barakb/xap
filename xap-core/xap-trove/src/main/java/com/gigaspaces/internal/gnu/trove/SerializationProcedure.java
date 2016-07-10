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

///////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2002, Eric D. Friedman All Rights Reserved.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////

package com.gigaspaces.internal.gnu.trove;

import java.io.IOException;
import java.io.ObjectOutput;

/**
 * Implementation of the variously typed procedure interfaces that supports writing the arguments to
 * the procedure out on an ObjectOutputStream. In the case of two-argument procedures, the arguments
 * are written out in the order received.
 *
 * <p> Any IOException is trapped here so that it can be rethrown in a writeObject method. </p>
 *
 * Created: Sun Jul  7 00:14:18 2002
 *
 * @author Eric D. Friedman
 * @version $Id: SerializationProcedure.java,v 1.5 2006/11/10 23:27:54 robeden Exp $
 */

class SerializationProcedure implements
        TIntObjectProcedure,
        TIntProcedure,
        TLongObjectProcedure,
        TLongProcedure,
        TShortLongProcedure,
        TShortObjectProcedure,
        TShortProcedure,
        TObjectIntProcedure,
        TObjectLongProcedure,
        TObjectShortProcedure,
        TObjectObjectProcedure,
        TObjectProcedure {

    private final ObjectOutput stream;
    IOException exception;

    SerializationProcedure(ObjectOutput stream) {
        this.stream = stream;
    }

    public boolean execute(short val) {
        try {
            stream.writeShort(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(int val) {
        try {
            stream.writeInt(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(long val) {
        try {
            stream.writeLong(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(Object val) {
        try {
            stream.writeObject(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(Object key, Object val) {
        try {
            stream.writeObject(key);
            stream.writeObject(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(Object key, short val) {
        try {
            stream.writeObject(key);
            stream.writeShort(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(Object key, int val) {
        try {
            stream.writeObject(key);
            stream.writeInt(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(Object key, long val) {
        try {
            stream.writeObject(key);
            stream.writeLong(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(int key, Object val) {
        try {
            stream.writeInt(key);
            stream.writeObject(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(long key, Object val) {
        try {
            stream.writeLong(key);
            stream.writeObject(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(short key, Object val) {
        try {
            stream.writeShort(key);
            stream.writeObject(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }

    public boolean execute(short key, long val) {
        try {
            stream.writeShort(key);
            stream.writeLong(val);
        } catch (IOException e) {
            this.exception = e;
            return false;
        }
        return true;
    }
}// SerializationProcedure
