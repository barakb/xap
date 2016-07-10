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

package com.gigaspaces.annotation.pojo;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Defines multiple CompoundSpaceIndex annotations for a POJO.<br> <p> For example: <p> <code>
 *
 * @author Yechiel Fefer
 * @CompoundSpaceIndexes( { @CompoundSpaceIndex(paths = {"personalInfo.id",
 * "counter"}),@CompoundSpaceIndex(paths = {"messageId","counter")) })
 *
 * </code>
 * @since 9.5
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface CompoundSpaceIndexes {
    CompoundSpaceIndex[] value();

}
