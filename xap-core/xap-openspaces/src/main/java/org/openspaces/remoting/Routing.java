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


package org.openspaces.remoting;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A method parameter annotation allowing to control the routing of a certain remote invocation
 * using an annotation. If annotated, will use the parameter as the routing index (determines which
 * partition it will "hit").
 *
 * <p>The annotation value allows to set an optional method name which will be invoked on the
 * parameter using reflection in order to get the actual routing index.
 *
 * @author kimchy
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface Routing {

    /**
     * Optional method name. If set, will call this method name using reflection in order to get the
     * actual routing index. If not set, will use the parameter value itself.
     */
    String value() default "";
}
