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

package org.openspaces.spatial;

import org.junit.Assert;
import org.junit.Test;
import org.openspaces.spatial.shapes.Shape;

import static org.openspaces.spatial.ShapeFactory.circle;
import static org.openspaces.spatial.ShapeFactory.lineString;
import static org.openspaces.spatial.ShapeFactory.point;
import static org.openspaces.spatial.ShapeFactory.polygon;
import static org.openspaces.spatial.ShapeFactory.rectangle;

public class ShapeFormatParseTest {

    @Test
    public void testWkt() {
        test(ShapeFormat.WKT);
    }

    @Test
    public void testGeoJson() {
        test(ShapeFormat.GEOJSON);
    }

    private void test(ShapeFormat shapeFormat) {
        Shape[] shapes = new Shape[]{
                point(1, 2),
                rectangle(1, 2, 3, 4),
                lineString(point(1, 11), point(2, 12), point(3, 13)),
                polygon(point(0, 0), point(0, 5), point(5, 0)),
                polygon(point(0, 0), point(0, 5), point(2.5, 2.5), point(5, 5), point(5, 0)),
                circle(point(0, 0), 5)
        };

        for (Shape shape : shapes) {
            String s = shape.toString(shapeFormat);
            System.out.println("Shape in format " + shapeFormat + ": " + s);
            Shape parsed = ShapeFactory.parse(s, shapeFormat);
            Assert.assertEquals(shape, parsed);
        }
    }
}
