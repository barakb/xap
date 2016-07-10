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
 * Parses space name
 *
 * @author Evgeny Fisher
 * @since 12.0
 */
public class SpaceNameParser {


    final public static String SPACE_NAME_PARAMETER = "name";

    /**
     * Parses space name -name parameter
     *
     * @param params The parameters to parse for - space name
     */
    public static String parse(CommandLineParser.Parameter[] params) throws IOException {

        String name = null;
        for (int i = 0; i < params.length; i++) {
            if (params[i].getName().equalsIgnoreCase(SPACE_NAME_PARAMETER)) {
                for (int j = 0; j < params[i].getArguments().length; j++) {
                    name = params[i].getArguments()[j];
                    break;
                }
            }
        }
        return name;
    }
}