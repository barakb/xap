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

/**
 *
 */
package com.j_spaces.kernel;

/**
 * @author anna
 * @since 6.1
 */
public interface IOrderedList<T> extends IStoredList<T> {

    /**
     * establish a scan position in one direction ascending (towards head) or descending (towards
     * tail).
     *
     * Used by the OrderedIndex masterList to establish an index scan.
     *
     * @param OrderedScanPivot a pivot from which to start this scan. if <tt>null</tt>, pivot is
     *                         Head (if ascending is true); Tail otherwise
     * @param ascending        if <code>true</code> scan from pivot towards head; if
     *                         <code>false</code> scan from pivot towards tail.
     * @return returns a  Holder of this scan. This Resource should be released if scan ended
     * prematurely.
     */
    public abstract IStoredListIterator<T> establishListOrderedScan(
            IObjectInfo<T> OrderedScanPivot, boolean ascending);

    /**
     * Given an ObjectInfo of an existing element, store a new element right "before" this one
     * (towards the tail). This Method used in OrderedIndex object when the MasterList reflects the
     * order of elements: head is the largest, tail the smallest in value.
     *
     * [Tail] - .. - [beforeCeiling] - [newOi] - [ceiling] - .. - [Head]
     *
     * @param ceiling a ceiling to the new element (towards the head); when <tt>null</tt>, the
     *                ceiling will be the Head.
     * @param subject a new element, to be inserted before the ceiling (towards the tail)
     * @return an information node representing the inserted element.
     *
     * <p><i><b>Note:</b> not relevant for random scans</i></p>
     * @throws RuntimeException if ceiling was already deleted
     */
    public abstract IObjectInfo<T> storeBeforeCeiling(IObjectInfo<T> ceiling,
                                                      T subject);

    /**
     * In an ordered , get the element larger than <tt>ref</tt>.
     *
     * @param ref a reference to an OI (element in the )
     * @return the next largest element to ref (towards head)
     */
    public abstract IObjectInfo<T> getLargerInOrder(IObjectInfo<T> ref);

    /**
     * In an ordered , get the element smaller than <tt>ref</tt>.
     *
     * @param ref a reference to an OI (element in the )
     * @return the next smallest to ref (towards tail)
     */
    public abstract IObjectInfo<T> getSmallerInOrder(IObjectInfo<T> ref);

}