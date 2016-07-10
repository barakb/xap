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

import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNSimulator;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SQL2MongoParser extends Parser {
    protected static final DFA[] _decisionToDFA;
    protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();
    public static final int T__11 = 1, T__10 = 2, T__9 = 3, T__8 = 4, T__7 = 5,
            T__6 = 6, T__5 = 7, T__4 = 8, T__3 = 9, T__2 = 10, T__1 = 11,
            T__0 = 12, OR = 13, AND = 14, NULL = 15, PRAM = 16, ID = 17,
            NAME = 18, WS = 19;
    public static final String[] tokenNames = {"<INVALID>", "'is'", "'>'",
            "'like'", "')'", "'('", "'rlike'", "'<'", "'='", "'>='", "'!='",
            "'NOT'", "'<='", "OR", "AND", "NULL", "'?'", "ID", "NAME", "WS"};
    public static final int RULE_parse = 0, RULE_expression = 1, RULE_or = 2,
            RULE_and = 3, RULE_not = 4, RULE_atom = 5, RULE_op = 6,
            RULE_value = 7;
    public static final String[] ruleNames = {"parse", "expression", "or",
            "and", "not", "atom", "op", "value"};

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
    public ATN getATN() {
        return _ATN;
    }

    public SQL2MongoParser(TokenStream input) {
        super(input);
        _interp = new ParserATNSimulator(this, _ATN, _decisionToDFA,
                _sharedContextCache);
    }

    public static class ParseContext extends ParserRuleContext {
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public TerminalNode EOF() {
            return getToken(SQL2MongoParser.EOF, 0);
        }

        public ParseContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_parse;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof SQL2MongoVisitor)
                return ((SQL2MongoVisitor<? extends T>) visitor)
                        .visitParse(this);
            else
                return visitor.visitChildren(this);
        }
    }

    public final ParseContext parse() throws RecognitionException {
        ParseContext _localctx = new ParseContext(_ctx, getState());
        enterRule(_localctx, 0, RULE_parse);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(16);
                expression();
                setState(17);
                match(EOF);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class ExpressionContext extends ParserRuleContext {
        public OrContext or() {
            return getRuleContext(OrContext.class, 0);
        }

        public ExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_expression;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof SQL2MongoVisitor)
                return ((SQL2MongoVisitor<? extends T>) visitor)
                        .visitExpression(this);
            else
                return visitor.visitChildren(this);
        }
    }

    public final ExpressionContext expression() throws RecognitionException {
        ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
        enterRule(_localctx, 2, RULE_expression);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(19);
                or();
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class OrContext extends ParserRuleContext {
        public TerminalNode OR(int i) {
            return getToken(SQL2MongoParser.OR, i);
        }

        public AndContext and(int i) {
            return getRuleContext(AndContext.class, i);
        }

        public List<TerminalNode> OR() {
            return getTokens(SQL2MongoParser.OR);
        }

        public List<AndContext> and() {
            return getRuleContexts(AndContext.class);
        }

        public OrContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_or;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof SQL2MongoVisitor)
                return ((SQL2MongoVisitor<? extends T>) visitor).visitOr(this);
            else
                return visitor.visitChildren(this);
        }
    }

    public final OrContext or() throws RecognitionException {
        OrContext _localctx = new OrContext(_ctx, getState());
        enterRule(_localctx, 4, RULE_or);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(21);
                and();
                setState(26);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == OR) {
                    {
                        {
                            setState(22);
                            match(OR);
                            setState(23);
                            and();
                        }
                    }
                    setState(28);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class AndContext extends ParserRuleContext {
        public List<NotContext> not() {
            return getRuleContexts(NotContext.class);
        }

        public List<TerminalNode> AND() {
            return getTokens(SQL2MongoParser.AND);
        }

        public TerminalNode AND(int i) {
            return getToken(SQL2MongoParser.AND, i);
        }

        public NotContext not(int i) {
            return getRuleContext(NotContext.class, i);
        }

        public AndContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_and;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof SQL2MongoVisitor)
                return ((SQL2MongoVisitor<? extends T>) visitor).visitAnd(this);
            else
                return visitor.visitChildren(this);
        }
    }

    public final AndContext and() throws RecognitionException {
        AndContext _localctx = new AndContext(_ctx, getState());
        enterRule(_localctx, 6, RULE_and);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(29);
                not();
                setState(34);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == AND) {
                    {
                        {
                            setState(30);
                            match(AND);
                            setState(31);
                            not();
                        }
                    }
                    setState(36);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class NotContext extends ParserRuleContext {
        public AtomContext atom() {
            return getRuleContext(AtomContext.class, 0);
        }

        public NotContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_not;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof SQL2MongoVisitor)
                return ((SQL2MongoVisitor<? extends T>) visitor).visitNot(this);
            else
                return visitor.visitChildren(this);
        }
    }

    public final NotContext not() throws RecognitionException {
        NotContext _localctx = new NotContext(_ctx, getState());
        enterRule(_localctx, 8, RULE_not);
        try {
            setState(40);
            switch (_input.LA(1)) {
                case 11:
                    enterOuterAlt(_localctx, 1);
                {
                    setState(37);
                    match(11);
                    setState(38);
                    atom();
                }
                break;
                case 5:
                case ID:
                    enterOuterAlt(_localctx, 2);
                {
                    setState(39);
                    atom();
                }
                break;
                default:
                    throw new NoViableAltException(this);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class AtomContext extends ParserRuleContext {
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class, 0);
        }

        public ValueContext value(int i) {
            return getRuleContext(ValueContext.class, i);
        }

        public List<OpContext> op() {
            return getRuleContexts(OpContext.class);
        }

        public List<ValueContext> value() {
            return getRuleContexts(ValueContext.class);
        }

        public TerminalNode ID() {
            return getToken(SQL2MongoParser.ID, 0);
        }

        public OpContext op(int i) {
            return getRuleContext(OpContext.class, i);
        }

        public AtomContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_atom;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof SQL2MongoVisitor)
                return ((SQL2MongoVisitor<? extends T>) visitor)
                        .visitAtom(this);
            else
                return visitor.visitChildren(this);
        }
    }

    public final AtomContext atom() throws RecognitionException {
        AtomContext _localctx = new AtomContext(_ctx, getState());
        enterRule(_localctx, 10, RULE_atom);
        int _la;
        try {
            setState(55);
            switch (_input.LA(1)) {
                case ID:
                    enterOuterAlt(_localctx, 1);
                {
                    setState(42);
                    match(ID);
                    setState(48);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << 1)
                            | (1L << 2) | (1L << 3) | (1L << 6) | (1L << 7)
                            | (1L << 8) | (1L << 9) | (1L << 10) | (1L << 12))) != 0)) {
                        {
                            {
                                setState(43);
                                op();
                                setState(44);
                                value();
                            }
                        }
                        setState(50);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    }
                }
                break;
                case 5:
                    enterOuterAlt(_localctx, 2);
                {
                    setState(51);
                    match(5);
                    setState(52);
                    expression();
                    setState(53);
                    match(4);
                }
                break;
                default:
                    throw new NoViableAltException(this);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class OpContext extends ParserRuleContext {
        public OpContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_op;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof SQL2MongoVisitor)
                return ((SQL2MongoVisitor<? extends T>) visitor).visitOp(this);
            else
                return visitor.visitChildren(this);
        }
    }

    public final OpContext op() throws RecognitionException {
        OpContext _localctx = new OpContext(_ctx, getState());
        enterRule(_localctx, 12, RULE_op);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(57);
                _la = _input.LA(1);
                if (!((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << 1)
                        | (1L << 2) | (1L << 3) | (1L << 6) | (1L << 7)
                        | (1L << 8) | (1L << 9) | (1L << 10) | (1L << 12))) != 0))) {
                    _errHandler.recoverInline(this);
                }
                consume();
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class ValueContext extends ParserRuleContext {
        public TerminalNode PRAM() {
            return getToken(SQL2MongoParser.PRAM, 0);
        }

        public TerminalNode NULL() {
            return getToken(SQL2MongoParser.NULL, 0);
        }

        public ValueContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_value;
        }

        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if (visitor instanceof SQL2MongoVisitor)
                return ((SQL2MongoVisitor<? extends T>) visitor)
                        .visitValue(this);
            else
                return visitor.visitChildren(this);
        }
    }

    public final ValueContext value() throws RecognitionException {
        ValueContext _localctx = new ValueContext(_ctx, getState());
        enterRule(_localctx, 14, RULE_value);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(59);
                _la = _input.LA(1);
                if (!(_la == NULL || _la == PRAM)) {
                    _errHandler.recoverInline(this);
                }
                consume();
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static final String _serializedATN = "\2\3\25@\4\2\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t"
            + "\t\3\2\3\2\3\2\3\3\3\3\3\4\3\4\3\4\7\4\33\n\4\f\4\16\4\36\13\4\3\5\3\5"
            + "\3\5\7\5#\n\5\f\5\16\5&\13\5\3\6\3\6\3\6\5\6+\n\6\3\7\3\7\3\7\3\7\7\7"
            + "\61\n\7\f\7\16\7\64\13\7\3\7\3\7\3\7\3\7\5\7:\n\7\3\b\3\b\3\t\3\t\3\t"
            + "\2\n\2\4\6\b\n\f\16\20\2\4\5\3\5\b\f\16\16\3\21\22<\2\22\3\2\2\2\4\25"
            + "\3\2\2\2\6\27\3\2\2\2\b\37\3\2\2\2\n*\3\2\2\2\f9\3\2\2\2\16;\3\2\2\2\20"
            + "=\3\2\2\2\22\23\5\4\3\2\23\24\7\1\2\2\24\3\3\2\2\2\25\26\5\6\4\2\26\5"
            + "\3\2\2\2\27\34\5\b\5\2\30\31\7\17\2\2\31\33\5\b\5\2\32\30\3\2\2\2\33\36"
            + "\3\2\2\2\34\32\3\2\2\2\34\35\3\2\2\2\35\7\3\2\2\2\36\34\3\2\2\2\37$\5"
            + "\n\6\2 !\7\20\2\2!#\5\n\6\2\" \3\2\2\2#&\3\2\2\2$\"\3\2\2\2$%\3\2\2\2"
            + "%\t\3\2\2\2&$\3\2\2\2\'(\7\r\2\2(+\5\f\7\2)+\5\f\7\2*\'\3\2\2\2*)\3\2"
            + "\2\2+\13\3\2\2\2,\62\7\23\2\2-.\5\16\b\2./\5\20\t\2/\61\3\2\2\2\60-\3"
            + "\2\2\2\61\64\3\2\2\2\62\60\3\2\2\2\62\63\3\2\2\2\63:\3\2\2\2\64\62\3\2"
            + "\2\2\65\66\7\7\2\2\66\67\5\4\3\2\678\7\6\2\28:\3\2\2\29,\3\2\2\29\65\3"
            + "\2\2\2:\r\3\2\2\2;<\t\2\2\2<\17\3\2\2\2=>\t\3\2\2>\21\3\2\2\2\7\34$*\62"
            + "9";
    public static final ATN _ATN = ATNSimulator.deserialize(_serializedATN
            .toCharArray());

    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    }
}