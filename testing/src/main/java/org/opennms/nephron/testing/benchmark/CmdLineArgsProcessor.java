/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2021 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2021 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.nephron.testing.benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.jparsec.Parser;
import org.jparsec.Parsers;
import org.jparsec.Scanners;
import org.jparsec.pattern.CharPredicate;
import org.jparsec.pattern.Patterns;

/**
 * Processes command line arguments into a corresponding argument expression and additional options.
 * <p>
 * For testing it is convenient to run the pipeline for several different parameter lists. There is a combinatorial
 * explosion if multiple parameters are varied. In order to define parameter lists succinctly, argument expressions
 * are used to describe possible argument lists. Simpler argument expressions can be combined by '&amp;' and '|'
 * combinators into more complex argument expressions. The following simple arguments expressions are supported:
 * <dl>
 *     <dt>--arg=simpleValue</dt>
 *     <dd>standard argument assignment</dd>
 *     <dt>--arg=(value1|value2|value3)</dt>
 *     <dd>enumerated alternative argument values</dd>
 *     <dt>--arg=(start#step#count)</dt>
 *     <dd>start, step, and count are long numbers that describe a set of values, namely (start, start+step, start+2*step, ...)</dd>
 * </dl>
 * Simple argument expressions can directly be supplied on the command line. Using the {@code -e "<expr>"} command
 * line option, argument expressions using the 'amp;' and '|' combinator can be supplied. E.g.:
 * <dl>
 *     <dt>{@code -e "--arg1=(a|b) & --arg2=(u|v)}</dt>
 *     <dd>Defines 4 parameter lists, namely: {@code "--arg1=a --arg2=u"}, {@code "--arg1=a --arg2=v"}, {@code "--arg1=b --arg2=u"}, and {@code "--arg1=b --arg2=v"}</dd>
 *     <dd>Note that the '&amp;' can be omitted, i.e. the same result is achieved by {@code -e "--arg1=(a|b) --arg2=(u|v)}</dd>
 *     <dt>{@code -e "--a=0 --b=1 | --a=1 --b=0}"</dt>
 *     <dd>Defines two parameter lists, namely: {@code --a=0 --b=1} and {@code --a=1 --b=0}</dd>
 * </dl>
 * It is also possible to store argument expressions in files and include these using an {@code --include=<file>}
 * argument. Argument expression files can contain multiple lines where each line represents an alternative list of
 * parameters. Empty lines and lines starting with a '#' character are skipped. Multiple alternative files can
 * also be included:
 * <dl>
 *     <dt>{@code --include=file1.arg}</dt>
 *     <dd>Includes a single file</dd>
 *     <dt>{@code --include(file1.arg|file2.arg)}</dt>
 *     <dd>Includes parameter lists from {@code file1.arg} and {@code file2.arg}. Parameter lists are treated as alternatives.</dd>
 * </dl>
 * In argument expression files the '&amp;' and '|' operator can be used. Argument expression files can also contain nested
 * {@code --include=...} arguments, thereby allowing modularization of argument expressions.
 * <p>
 * If an argument is specified several times then the first occurrence takes precedence. This allows to override argument
 * values in argument files by specifying their values on the command line before {@code --include=...} arguments.
 * <p>
 * The benchmark launcher supports a couple of options that can only be used directly on the command line (i.e. not
 * in argument expressions). All of these options start with a single '-'. These options are:
 * <dl>
 *     <dt>{@code -e "expr"}</dt>
 *     <dd>Specifies argument definition expression. (Can be used multiple times.)</dd>
 *     <dt>{@code -before <commmand>}</dt>
 *     <dd>A command that is executed before each pipeline run.</dd>
 *     <dt>{@code -after <commmand>}</dt>
 *     <dd>A command that is executed after each pipeline run.</dd>
 *     <dt>{@code -out <file>}</dt>
 *     <dd>A file where execution times, parameters, and results are recorded.</dd>
 * </dl>
 */
public class CmdLineArgsProcessor {

    public static final CharPredicate IS_SIMPLE_VALUE = new CharPredicate() {
        @Override public boolean isChar(char c) {
            return !Character.isWhitespace(c) && c != '(' && c != ')' && c != '&' && c != '|';
        }
        @Override public String toString() {
            return "no-ws";
        }
    };

    static final Parser<Void> ddash = Scanners.string("--");

    // whitespace that is kept because it stands for an omitted '&' operator
    private static final Parser<Void> relevantWhitespace =
            Scanners.WHITESPACES.notFollowedBy(ddash.or(Scanners.isChar('(')));

    private static final <T> Parser<T> withSurroundingWhitespace(Parser<T> p) {
        return Scanners.WHITESPACES.optional(null).next(p).followedBy(Scanners.WHITESPACES.optional(null));
    }

    static final Parser<Void> equals = Scanners.isChar('=');
    static final Parser<Void> open = withSurroundingWhitespace(Scanners.isChar('('));

    // whitespace after a closing parenthesis is consumed only if it is not followed by a "--"
    // -> the whitespace is interpreted as an omitted '&' operator
    static final Parser<Void> close =
            Scanners.WHITESPACES.optional(null).next(Scanners.isChar(')')).followedBy(relevantWhitespace.optional(null));

    static final Parser<Void> and = Parsers.or(
            withSurroundingWhitespace(Scanners.isChar('&')),
            Scanners.WHITESPACES,
            Scanners.isChar('(').peek(),
            ddash.peek()
    );
    static final Parser<Void> or = withSurroundingWhitespace(Scanners.isChar('|'));
    static final Parser<Void> hash = withSurroundingWhitespace(Scanners.isChar('#'));

    static final Parser<String> valueAlternative =  Scanners.notAmong("#|)").many().source();

    static final Parser<ArgValue> singleArgValueParser =
            Patterns.many(IS_SIMPLE_VALUE).toScanner("value").source().map(string -> new ArgValue.Simple(string));

    static final Parser<ArgValue> alternativesArgValueParser =
            open.next(valueAlternative.sepBy1(or)).followedBy(close).map(list -> new ArgValue.Alternatives(list));

    static final Parser<Long> longParser =
            (Scanners.isChar('-').optional(null).next(Scanners.among("0123456789").many())).source().map(s -> Long.parseLong(s));

    static final Parser<ArgValue> stepsArgValueParser =
            open.next(longParser.sepBy1(hash)).followedBy(close).map(list -> new ArgValue.Steps(list));

    static final Parser<ArgValue> argValueParser = alternativesArgValueParser.or(stepsArgValueParser).or(singleArgValueParser);

    static final Parser<Args> singleArgParser =
            ddash.next(Parsers.pair(Scanners.IDENTIFIER.source().followedBy(equals), argValueParser).map(pair -> Args.simpleOrInclude(pair.a, pair.b)));

    static final Parser.Reference<Args> orRef = Parser.newReference();

    static final Parser<Args> baseArgParser = singleArgParser.or(open.next(orRef.lazy()).followedBy(close));

    static final Parser<Args> andArgParser = baseArgParser.sepBy1(and).map(list -> (Args)new Args.And(list)).label("ands");

    static final Parser<Args> orArgParser = andArgParser.sepBy1(or).map(list -> (Args)new Args.Or(list)).label("ors");

    static {
        orRef.set(orArgParser);
    }

    /**
     * Processes the given command line arguments and derives pipeline arguments as well as additional options.
     */
    public static CmdLineArgs process(String... strings) {
        List<Args> args = new ArrayList<>();
        String before = null, after = null, output = null;
        var idx = 0;
        BiFunction<Integer, String, String> nextArg = (i, option) -> {
            if (i == strings.length - 1) {
                throw new RuntimeException("missing argument after -" + option + " option");
            } else {
                return strings[i + 1];
            }
        };
        while (idx < strings.length) {
            if ("-e".equals(strings[idx])) {
                String s = nextArg.apply(idx, "e");
                try {
                    args.add(orArgParser.parse(s));
                } catch (Exception e) {
                    throw new RuntimeException("can not parse argument expression: " + s, e);
                }
                idx += 2;
            } else if ("-before".equals(strings[idx])) {
                before = nextArg.apply(idx, "before");
                idx += 2;
            } else if ("-after".equals(strings[idx])) {
                after = nextArg.apply(idx, "after");
                idx += 2;
            } else if ("-out".equals(strings[idx])) {
                output = nextArg.apply(idx, "out");
                idx += 2;
            } else {
                try {
                    args.add(singleArgParser.parse(strings[idx]));
                } catch (Exception e) {
                    throw new RuntimeException("can not parse simple argument expression: " + strings[idx], e);
                }
                idx += 1;
            }
        }
        Args pipelineArgs;
        if (args.size() == 1) {
            pipelineArgs = args.get(0);
        } else {
            pipelineArgs = new Args.And(args);
        }
        return new CmdLineArgs(before, after, output, pipelineArgs);
    }

    private static Args include(String file) {
        try {
            try (var reader = new BufferedReader(new FileReader(file))) {
                List<Args> args = new ArrayList<>();
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (trimmed.isBlank() || trimmed.startsWith("#")) continue;
                    var arg = orArgParser.parse(trimmed);
                    args.add(arg);
                }
                if (args.size() == 1) {
                    return args.get(0);
                } else {
                    return new Args.Or(args);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static class CmdLineArgs {
        public final String before;
        public final String after;
        public final String out;
        public final Args args;

        public CmdLineArgs(String before, String after, String out, Args args) {
            this.before = before;
            this.after = after;
            this.out = out;
            this.args = args;
        }
    }

    /**
     * Represents an expression for specifying pipeline parameters. The expression can be evaluated yielding the
     * specified parameter sets.
     */
    public static abstract class Args {

        public abstract Params eval();

        public static Args simpleOrInclude(String name, ArgValue value) {
            if ("include".equals(name)) {
                if (value instanceof ArgValue.Simple) {
                    return include(((ArgValue.Simple)value).string);
                } else if (value instanceof ArgValue.Alternatives) {
                    var includes = ((ArgValue.Alternatives)value).strings.stream().map(s -> include(s)).collect(Collectors.toList());
                    return new Or(includes);
                } else {
                    throw new RuntimeException("--include can not operate on step values");
                }
            } else {
                return new Simple(name, value);
            }
        }

        public static class Simple extends Args {
            private final String name;
            private final ArgValue value;
            public Simple(String name, ArgValue value) {
                this.name = name;
                this.value = value;
            }

            @Override
            public Params eval() {
                return new Params.Or(value.expand().stream().map(v -> Params.and("--" + name + "=" + v)).collect(Collectors.toList()));
            }
        }

        public static class And extends Args {
            private final List<Args> args;

            public And(List<Args> args) {
                this.args = args;
            }

            @Override
            public Params eval() {
                return args.stream().map(a -> a.eval()).reduce((p1, p2) -> p1.and(p2)).orElse(Params.or());
            }
        }

        public static class Or extends Args {
            private final List<? extends Args> args;

            public Or(List<? extends Args> args) {
                this.args = args;
            }

            @Override
            public Params eval() {
                return args.stream().map(a -> a.eval()).reduce((p1, p2) -> p1.or(p2)).orElse(Params.or());
            }
        }

    }

    public static abstract class ArgValue {

        public abstract List<String> expand();

        public static class Simple extends ArgValue {
            private final String string;

            public Simple(String string) {
                this.string = string;
            }

            @Override
            public List<String> expand() {
                return Collections.singletonList(string);
            }
        }

        public static class Alternatives extends ArgValue {
            private final List<String> strings;

            public Alternatives(List<String> strings) {
                this.strings = strings;
            }

            @Override
            public List<String> expand() {
                return strings;
            }
        }

        public static class Steps extends ArgValue {
            private final long from, step, count;
            public Steps(List<Long> list) {
                from = list.get(0);
                step = list.get(1);
                count = list.get(2);
            }

            @Override
            public List<String> expand() {
                List<String> res = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    res.add(String.valueOf(from + i * step));
                }
                return res;
            }
        }
    }

    /**
     * Represents parameter lists for pipeline execution.
     */
    public static abstract class Params {

        public static Params.And and(String... strings) {
            return new Params.And(Arrays.asList(strings));
        }

        public static Params.Or or(String... strings) {
            return new Params.Or(Arrays.stream(strings).map(s -> and(s)).collect(Collectors.toList()));
        }

        public abstract Params or(Params other);
        public abstract Params and(Params other);

        /**
         * Returns a list of all possible parameter lists.
         */
        public abstract List<List<String>> expand();

        public static class Or extends Params {

            private final List<And> ands;

            public Or(List<And> ands) {
                this.ands = ands;
            }

            @Override
            public Params or(Params other) {
                if (other instanceof Or) {
                    return new Or(ListUtils.union(ands, ((Or)other).ands));
                } else {
                    return new Or(ListUtils.union(ands, Collections.singletonList((And)other)));
                }
            }

            @Override
            public Params and(Params other) {
                if (other instanceof Or) {
                    return new Or(ands.stream().flatMap(al -> ((Or)other).ands.stream().map(ar -> al.add(ar))).collect(Collectors.toList()));
                } else {
                    return new Or(ands.stream().map(a -> a.add((And)other)).collect(Collectors.toList()));
                }
            }

            @Override
            public List<List<String>> expand() {
                return ands.stream().map(a -> a.values).collect(Collectors.toList());
            }
        }

        public static class And extends Params {
            private final List<String> values;
            public And(List<String> values) {
                this.values = values;
            }

            @Override
            public Params or(Params other) {
                if (other instanceof Or) {
                    return new Or(ListUtils.union(Collections.singletonList(this), ((Or)other).ands));
                } else {
                    return new Or(Arrays.asList(this, (And)other));
                }
            }

            @Override
            public Params and(Params other) {
                if (other instanceof Or) {
                    return new Or(((Or)other).ands.stream().map(a -> this.add(a)).collect(Collectors.toList()));
                } else {
                    return add((And)other);
                }
            }

            public And add(And other) {
                // keep arguments from "other" only if there is not already a setting for the same argument name
                // -> arguments that are set first (i.e. on the left) take precedence
                var argNames = values.stream().map(And::argName).collect(Collectors.toSet());
                var filtered = other.values.stream().filter(v -> !argNames.contains(argName(v))).collect(Collectors.toList());
                return new And(ListUtils.union(values, filtered));
            }

            @Override
            public List<List<String>> expand() {
                return Collections.singletonList(values);
            }

            private static String argName(String s) {
                return s.substring(0, s.indexOf('='));
            }
        }

    }

}
