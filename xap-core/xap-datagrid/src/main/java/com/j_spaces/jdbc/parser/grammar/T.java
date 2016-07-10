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

package com.j_spaces.jdbc.parser.grammar;

import com.j_spaces.jdbc.Query;

import java.io.ByteArrayInputStream;

/**
 * Created by Barak Bar Orion on 1/17/16.
 *
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class T {
    public static void main(String[] args) throws ParseException {
        T t = new T();
//        "A=B"
//        "A=B+1" *
//        "A=1"
//        "ABS(A)=10"
//        "ABS(A + 1)=10" *


//        Query query = t.parseString("A=B");
//        Query query = t.parseString("A < B");
//        Query query = t.parseString("ABS(A)=10");
//        Query query = t.parseString("ABS(A) < ?");
//        Query query = t.parseString("ABS(A) = 10");
//        Query query = t.parseString("ABS(A) = B");
        Query query = t.parseString("ABS(A) < B");
        System.out.println("query is " + query);
    }

    public Query parseString(String input) throws ParseException {
        SqlParser parser = new SqlParser(new ByteArrayInputStream(input.getBytes()));
        return parser.readMultipleQuery();
    }
}
