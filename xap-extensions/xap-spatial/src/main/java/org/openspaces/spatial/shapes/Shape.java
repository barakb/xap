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

package org.openspaces.spatial.shapes;

import org.openspaces.spatial.ShapeFormat;

import java.io.Serializable;

/**
 * Markup interface to serve as base for all spatial shapes supported by XAP.
 *
 * @author Yohana Khoury
 * @since 11.0
 */
public interface Shape extends Serializable {
    /**
     * Returns a string representation of the shape using the specified format.
     *
     * @param shapeFormat The format which will be used to format the shape.
     * @return A string representation of the shape.
     */
    String toString(ShapeFormat shapeFormat);

    /**
     * Appends a string representation of the shape using the specified format
     *
     * @param stringBuilder The string builder to append to
     * @param shapeFormat   The format which will be used to format the shape
     * @return The string builder
     */
    StringBuilder appendTo(StringBuilder stringBuilder, ShapeFormat shapeFormat);
}
