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
// Copyright (c) 2001, Eric D. Friedman All Rights Reserved.
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

//////////////////////////////////////////////////
// THIS IS A GENERATED CLASS. DO NOT HAND EDIT! //
//////////////////////////////////////////////////


/**
 * Iterator for int collections.
 *
 * @author Eric D. Friedman
 * @version $Id: PIterator.template,v 1.1 2006/11/10 23:28:00 robeden Exp $
 */

public class TIntIterator extends TPrimitiveIterator {
    /**
     * the collection on which the iterator operates
     */
    private final TIntHash _hash;

    /**
     * Creates a TIntIterator for the elements in the specified collection.
     */
    public TIntIterator(TIntHash hash) {
        super(hash);
        this._hash = hash;
    }

    /**
     * Advances the iterator to the next element in the underlying collection and returns it.
     *
     * @return the next int in the collection
     * @throws NoSuchElementException if the iterator is already exhausted
     */
    public int next() {
        moveToNextIndex();
        return _hash._set[_index];
    }
}// TIntIterator
