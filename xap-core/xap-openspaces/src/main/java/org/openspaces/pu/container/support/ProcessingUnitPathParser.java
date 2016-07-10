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

package org.openspaces.pu.container.support;

import java.io.IOException;

/**
 * Parses processing unit path
 *
 * @since 12.0
 */
public class ProcessingUnitPathParser {


    final public static String PATH_PARAMETER = "path";

    /**
     * Parses -path parameter
     *
     * @param params The parameters to parse for - path
     */
    public static String parse(CommandLineParser.Parameter[] params) throws IOException {

        String path = null;
        for (int i = 0; i < params.length; i++) {
            if (params[i].getName().equalsIgnoreCase(PATH_PARAMETER)) {
                for (int j = 0; j < params[i].getArguments().length; j++) {
                    path = params[i].getArguments()[j];
                    break;
                }
            }
        }
        return path;
    }
}