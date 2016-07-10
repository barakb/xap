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


package org.openspaces.core.context;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows to directly inject a {@link org.openspaces.core.GigaSpace} implementation into a class
 * field or setter property. A name can be specified in cases where more than one {@link
 * org.openspaces.core.GigaSpace} are defined within a spring application context. The name will be
 * the bean name / id. This annotation will cause the GigaSpace instance to be injected late during
 * the bean lifecycle (after its properties were set). It allows to break cyclic relationships on
 * beans that needs a GigaSpace instance but must be initialized before the space.
 *
 * @author kimchy
 */
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface GigaSpaceLateContext {

    /**
     * The name of the {@link org.openspaces.core.GigaSpace} bean. Used when more than one {@link
     * org.openspaces.core.GigaSpace} is defined and corresponds to the bean name / id.
     */
    String name() default "";
}