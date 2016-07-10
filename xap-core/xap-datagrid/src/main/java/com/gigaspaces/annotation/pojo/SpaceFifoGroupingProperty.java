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

import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.TakeModifiers;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * <pre>
 * Defines a space fifo grouping property.
 * Can be defined on a property getter.
 *
 * Fifo grouping property on nested object property is defined on the nested object getter.
 *
 * If defined, the {@link TakeModifiers#FIFO_GROUPING_POLL} or {@link
 * ReadModifiers#FIFO_GROUPING_POLL} modifiers can be used to get
 * the take/read results in fifo order, when the fifo poll is grouped by the property marked as
 * <code>SpaceFifoGroupingProperty</code>.
 *
 * For example:
 *  To index the 'socialSecurity' property
 *  <code>
 *  1. 	@spaceFifoGroupingProperty
 *  	public long getSocialSecurity() {
 *  		return socialSecurity;
 *    }
 *  </code>
 *  To index 'personalInfo.name':
 *  <code>
 *  2. 	@spaceFifoGroupingProperty(path = "name")
 *     	public Info getPersonalInfo() {
 * return personalInfo;
 * }
 * 	</code>
 * </pre>
 *
 * @author yael
 * @since 9.0
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface SpaceFifoGroupingProperty {
    public static final String EMPTY = "";

    String path() default EMPTY;

}
