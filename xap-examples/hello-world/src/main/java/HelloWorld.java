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

import com.j_spaces.core.LeaseContext;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import java.util.Arrays;
import java.util.Properties;

public class HelloWorld {

    public static void main(String[] args) {

        //extract command-line arguments
        Properties properties = validateArgs(args);

        // Get a reference to the data-grid
        GigaSpace space = getGigaSpace(properties);

        //write "Hello" and "World!" to the data-grid
        write(space, new Message("Hello"));
        write(space, new Message("World!"));

        //read "Hello" and "World!" from the data-grid
        read(space, new Message());

        //shutdown
        System.exit(0);
    }

    /*
     * parse arguments and return a mapping of option:value (e.g. -name=myDataGrid, -mode=embedded)
     */
    private static Properties validateArgs(String[] args) {
        final String usage = "usage: -name {data-grid name} -mode {embedded,remote}";
        if (args.length == 0) {
            throw new IllegalArgumentException(usage);
        }
        Properties properties = new Properties();
        for (int i = 0; i < args.length; i += 2) {
            if (args[i].equals("-name")) {
                properties.put("-name", args[i + 1]);
            } else if (args[i].equals("-mode")) {
                if (args[i + 1].equals("embedded") || args[i + 1].equals("remote")) {
                    properties.put("-mode", args[i + 1]);
                } else {
                    throw new IllegalArgumentException("unexpected argument `" + args[i + 1] + "` - " + usage);
                }
            } else {
                throw new IllegalArgumentException("unexpected argument `" + args[i] + "` - " + usage);
            }
        }

        System.out.println("Using properties: " + properties);
        return properties;
    }

    /*
     * either start an embedded data-grid (same JVM as client) or connect to a remote data-grid (by it's name)
     */
    private static GigaSpace getGigaSpace(Properties properties) {

        final String spaceName = (String) properties.get("-name");

        if (properties.get("-mode").equals("embedded")) {

            //Create an embedded data-grid instance
            GigaSpace space = new GigaSpaceConfigurer(new EmbeddedSpaceConfigurer(spaceName)).gigaSpace();
            System.out.println("Created embedded data-grid: " + spaceName);
            return space;

        } else if (properties.get("-mode").equals("remote")) {

            //Connect to a remote data-grid
            GigaSpace space = new GigaSpaceConfigurer(new SpaceProxyConfigurer(spaceName)).gigaSpace();
            System.out.println("Connected to remote data-grid: " + spaceName);
            return space;

        } else {
            throw new IllegalArgumentException("unexpected parsing of properties: " + properties);
        }
    }

    /*
     * Write (or update) an entity in the data-grid
     */
    private static void write(GigaSpace space, Message message) {
        LeaseContext<Message> context = space.write(message);

        if (context.getVersion() == 1) {
            System.out.println("write - " + message);
        } else {
            System.out.println("update - " + message);
        }
    }

    /*
     * Read a matching entity from the data-grid
     * Template matching is done by field equality or any if field is null
     */
    private static void read(GigaSpace space, Message msgTemplate) {
        Message[] results = space.readMultiple(msgTemplate);
        System.out.println("read - " + Arrays.toString(results));
    }
}
