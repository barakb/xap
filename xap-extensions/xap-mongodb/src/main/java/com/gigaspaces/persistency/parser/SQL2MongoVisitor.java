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

package com.gigaspaces.persistency.parser;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

public interface SQL2MongoVisitor<T> extends ParseTreeVisitor<T> {
    T visitNot(SQL2MongoParser.NotContext ctx);

    T visitExpression(SQL2MongoParser.ExpressionContext ctx);

    T visitAtom(SQL2MongoParser.AtomContext ctx);

    T visitOp(SQL2MongoParser.OpContext ctx);

    T visitOr(SQL2MongoParser.OrContext ctx);

    T visitValue(SQL2MongoParser.ValueContext ctx);

    T visitParse(SQL2MongoParser.ParseContext ctx);

    T visitAnd(SQL2MongoParser.AndContext ctx);
}