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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNSimulator;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SQL2MongoLexer extends Lexer {
    protected static final DFA[] _decisionToDFA;
    protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();
    public static final int T__11 = 1, T__10 = 2, T__9 = 3, T__8 = 4, T__7 = 5,
            T__6 = 6, T__5 = 7, T__4 = 8, T__3 = 9, T__2 = 10, T__1 = 11,
            T__0 = 12, OR = 13, AND = 14, NULL = 15, PRAM = 16, ID = 17,
            NAME = 18, WS = 19;
    public static String[] modeNames = {"DEFAULT_MODE"};

    public static final String[] tokenNames = {"<INVALID>", "'is'", "'>'",
            "'like'", "')'", "'('", "'rlike'", "'<'", "'='", "'>='", "'!='",
            "'NOT'", "'<='", "OR", "AND", "NULL", "'?'", "ID", "NAME", "WS"};
    public static final String[] ruleNames = {"T__11", "T__10", "T__9",
            "T__8", "T__7", "T__6", "T__5", "T__4", "T__3", "T__2", "T__1",
            "T__0", "OR", "AND", "NULL", "PRAM", "ID", "NAME", "WS"};

    public SQL2MongoLexer(CharStream input) {
        super(input);
        _interp = new LexerATNSimulator(this, _ATN, _decisionToDFA,
                _sharedContextCache);
    }

    @Override
    public String getGrammarFileName() {
        return "SQL2Mongo.g4";
    }

    @Override
    public String[] getTokenNames() {
        return tokenNames;
    }

    @Override
    public String[] getRuleNames() {
        return ruleNames;
    }

    @Override
    public String[] getModeNames() {
        return modeNames;
    }

    @Override
    public ATN getATN() {
        return _ATN;
    }

    @Override
    public void action(RuleContext _localctx, int ruleIndex, int actionIndex) {
        switch (ruleIndex) {
            case 18:
                WS_action((RuleContext) _localctx, actionIndex);
                break;
        }
    }

    private void WS_action(RuleContext _localctx, int actionIndex) {
        switch (actionIndex) {
            case 0:
                skip();
                break;
        }
    }

    public static final String _serializedATN = "\2\4\25z\b\1\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4"
            + "\t\t\t\4\n\t\n\4\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20"
            + "\4\21\t\21\4\22\t\22\4\23\t\23\4\24\t\24\3\2\3\2\3\2\3\3\3\3\3\4\3\4\3"
            + "\4\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\t\3\t\3\n"
            + "\3\n\3\n\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\r\3\r\3\r\3\16\3\16\3\16\3\17"
            + "\3\17\3\17\3\17\3\20\3\20\3\20\5\20Y\n\20\3\20\6\20\\\n\20\r\20\16\20"
            + "]\3\20\3\20\3\20\3\20\3\20\3\21\3\21\3\22\3\22\3\22\7\22j\n\22\f\22\16"
            + "\22m\13\22\3\23\6\23p\n\23\r\23\16\23q\3\24\6\24u\n\24\r\24\16\24v\3\24"
            + "\3\24\2\25\3\3\1\5\4\1\7\5\1\t\6\1\13\7\1\r\b\1\17\t\1\21\n\1\23\13\1"
            + "\25\f\1\27\r\1\31\16\1\33\17\1\35\20\1\37\21\1!\22\1#\23\1%\24\1\'\25"
            + "\2\3\2\t\4QQqq\4TTtt\4CCcc\4PPpp\4FFff\5\62;C\\c|\5\13\f\17\17\"\"~\2"
            + "\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2"
            + "\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2"
            + "\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2"
            + "\2\2%\3\2\2\2\2\'\3\2\2\2\3)\3\2\2\2\5,\3\2\2\2\7.\3\2\2\2\t\63\3\2\2"
            + "\2\13\65\3\2\2\2\r\67\3\2\2\2\17=\3\2\2\2\21?\3\2\2\2\23A\3\2\2\2\25D"
            + "\3\2\2\2\27G\3\2\2\2\31K\3\2\2\2\33N\3\2\2\2\35Q\3\2\2\2\37X\3\2\2\2!"
            + "d\3\2\2\2#f\3\2\2\2%o\3\2\2\2\'t\3\2\2\2)*\7k\2\2*+\7u\2\2+\4\3\2\2\2"
            + ",-\7@\2\2-\6\3\2\2\2./\7n\2\2/\60\7k\2\2\60\61\7m\2\2\61\62\7g\2\2\62"
            + "\b\3\2\2\2\63\64\7+\2\2\64\n\3\2\2\2\65\66\7*\2\2\66\f\3\2\2\2\678\7t"
            + "\2\289\7n\2\29:\7k\2\2:;\7m\2\2;<\7g\2\2<\16\3\2\2\2=>\7>\2\2>\20\3\2"
            + "\2\2?@\7?\2\2@\22\3\2\2\2AB\7@\2\2BC\7?\2\2C\24\3\2\2\2DE\7#\2\2EF\7?"
            + "\2\2F\26\3\2\2\2GH\7P\2\2HI\7Q\2\2IJ\7V\2\2J\30\3\2\2\2KL\7>\2\2LM\7?"
            + "\2\2M\32\3\2\2\2NO\t\2\2\2OP\t\3\2\2P\34\3\2\2\2QR\t\4\2\2RS\t\5\2\2S"
            + "T\t\6\2\2T\36\3\2\2\2UV\7P\2\2VW\7Q\2\2WY\7V\2\2XU\3\2\2\2XY\3\2\2\2Y"
            + "[\3\2\2\2Z\\\7\"\2\2[Z\3\2\2\2\\]\3\2\2\2][\3\2\2\2]^\3\2\2\2^_\3\2\2"
            + "\2_`\7p\2\2`a\7w\2\2ab\7n\2\2bc\7n\2\2c \3\2\2\2de\7A\2\2e\"\3\2\2\2f"
            + "k\5%\23\2gh\7\60\2\2hj\5%\23\2ig\3\2\2\2jm\3\2\2\2ki\3\2\2\2kl\3\2\2\2"
            + "l$\3\2\2\2mk\3\2\2\2np\t\7\2\2on\3\2\2\2pq\3\2\2\2qo\3\2\2\2qr\3\2\2\2"
            + "r&\3\2\2\2su\t\b\2\2ts\3\2\2\2uv\3\2\2\2vt\3\2\2\2vw\3\2\2\2wx\3\2\2\2"
            + "xy\b\24\2\2y(\3\2\2\2\b\2X]kqv";
    public static final ATN _ATN = ATNSimulator.deserialize(_serializedATN
            .toCharArray());

    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    }
}