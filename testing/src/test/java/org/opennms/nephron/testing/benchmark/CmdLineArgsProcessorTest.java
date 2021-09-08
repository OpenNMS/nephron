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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.opennms.nephron.testing.benchmark.CmdLineArgsProcessor.alternativesArgValueParser;
import static org.opennms.nephron.testing.benchmark.CmdLineArgsProcessor.and;
import static org.opennms.nephron.testing.benchmark.CmdLineArgsProcessor.longParser;
import static org.opennms.nephron.testing.benchmark.CmdLineArgsProcessor.or;
import static org.opennms.nephron.testing.benchmark.CmdLineArgsProcessor.orArgParser;
import static org.opennms.nephron.testing.benchmark.CmdLineArgsProcessor.singleArgParser;
import static org.opennms.nephron.testing.benchmark.CmdLineArgsProcessor.stepsArgValueParser;
import static org.opennms.nephron.testing.benchmark.CmdLineArgsProcessor.valueAlternative;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.Matchers;
import org.junit.Test;

public class CmdLineArgsProcessorTest {

    @Test
    public void basic() {
        assertThat(1l, is(longParser.parse("1")));
        assertThat(123l, is(longParser.parse("123")));
        assertThat(-123l, is(longParser.parse("-123")));
    }

    @Test
    public void operators() {
        and.parse("&");
        and.parse(" "); // the '&' can be omitted
        and.parse("& ");
        and.parse(" &");
        and.parse(" & ");
        or.parse("|");
        or.parse(" |");
        or.parse("| ");
        or.parse(" | ");
    }

    @Test
    public void or() {
        or.parse("|");
        or.parse(" |");
        or.parse("| ");
        or.parse(" | ");
    }

    @Test
    public void simpleArg() {
        assertThat(valueAlternative.parse("1"), is("1"));
        assertThat(valueAlternative.parse("100"), is("100"));
        assertThat(alternativesArgValueParser.parse("(1|100)").expand(), hasSize(2));
        List<String> steps = stepsArgValueParser.parse("(5#10#4)").expand();
        assertThat(steps, hasSize(4));
        var sum = steps.stream().mapToLong(Long::parseLong).sum();
        assertThat(sum, is(80l)); // 5 + 15 + 25 + 35
        assertThat(singleArgParser.parse("--cortexMaxBatchSize=(1|100)").eval().expand(), hasSize(2));
    }

    private File tmpFile() throws Exception {
        var file = File.createTempFile("test", ".arg");
        file.deleteOnExit();
        return file;
    }

    @Test
    public void include() throws Exception {
        var file1 = tmpFile();
        var file2 = tmpFile();
        try (var fw1 = new FileWriter(file1, StandardCharsets.UTF_8)) {
            try (var fw2 = new FileWriter(file2, StandardCharsets.UTF_8)) {
                fw1.write("--a=2 --b=2 --include=" + file2.getCanonicalPath());
                fw2.write("--a=3 --b=3 --c=3");
            }
        }
        var cmdLineArgs = CmdLineArgsProcessor.process("--a=1", "--include=" + file1.getCanonicalPath());
        check(cmdLineArgs.args, set(set("--a=1", "--b=2", "--c=3")));
    }

    @Test
    public void simpleCmdLineArgs() {
        check(args("--runner=FlinkRunner", "--test=10"), set(set("--runner=FlinkRunner", "--test=10")));
        check(args("--cortexMaxBatchSize=(1|100)"), set(set("--cortexMaxBatchSize=1"), set("--cortexMaxBatchSize=100")));
    }

    @Test
    public void expressions() {
        check(args("-e", "--runner=FlinkRunner & --test=10"), set(set("--runner=FlinkRunner", "--test=10")));
        // the "and" operator is optional
        check(args("-e", "--runner=FlinkRunner --test=10"), set(set("--runner=FlinkRunner", "--test=10")));
        check(args("-e", "--runner=FlinkRunner | --test=10"), set(set("--runner=FlinkRunner"), set("--test=10")));
        check(args("-e", "--abc=(xyz|uvw)"), set(set("--abc=xyz"), set("--abc=uvw")));
        check(args("-e", "--a=(1#1#4)"), set(set("--a=1"), set("--a=2"), set("--a=3"), set("--a=4")));
        check(args("-e", "--a=(x|y) --b=u"), set(set("--a=x", "--b=u"), set("--a=y", "--b=u")));
    }

    @Test
    public void orsOfAnds() {
        check(args("-e", "--a=1 --b=2 | --a=2 --b=3"), set(set("--a=1", "--b=2"), set("--a=2", "--b=3")));
    }

    @Test
    public void andOfOrs() {
        check(
                args("-e", "--a=(1|2) & --b=(3|4)"),
                set(set("--a=1", "--b=3"), set("--a=1", "--b=4"), set("--a=2", "--b=3"), set("--a=2", "--b=4"))
        );
        check(
                args("-e", "--a=(0#1#4) & --b=(x|y)"),
                set(
                        set("--a=0", "--b=x"), set("--a=0", "--b=y"),
                        set("--a=1", "--b=x"), set("--a=1", "--b=y"),
                        set("--a=2", "--b=x"), set("--a=2", "--b=y"),
                        set("--a=3", "--b=x"), set("--a=3", "--b=y")
                )
        );
        check(
                args("-e", "--a=(0#1#4) --b=(x|y)"),
                set(
                        set("--a=0", "--b=x"), set("--a=0", "--b=y"),
                        set("--a=1", "--b=x"), set("--a=1", "--b=y"),
                        set("--a=2", "--b=x"), set("--a=2", "--b=y"),
                        set("--a=3", "--b=x"), set("--a=3", "--b=y")
                )
        );
    }

    @Test
    public void parentheses() {
        check(orArgParser.parse("(--a=1)"), set(set("--a=1")));
        check(orArgParser.parse("(--a=1) (--b=2)"), set(set("--a=1", "--b=2")));
        check(orArgParser.parse("(--a=1) --b=2"), set(set("--a=1", "--b=2")));
        check(orArgParser.parse("--a=1 (--b=2)"), set(set("--a=1", "--b=2")));
        check(orArgParser.parse("(--a=1)(--b=2)"), set(set("--a=1", "--b=2")));
        check(orArgParser.parse("(--a=1)--b=2"), set(set("--a=1", "--b=2")));
        check(orArgParser.parse("--a=1(--b=2)"), set(set("--a=1", "--b=2")));
    }

    @Test
    public void precedence() {
        // the first value take precedence
        check(orArgParser.parse("--a=1 --a=2"), set(set("--a=1")));
        check(orArgParser.parse("--a=1 | --a=2 --a=3"), set(set("--a=1"), set("--a=2")));
        check(orArgParser.parse("--a=1 --a=2 | --a=3"), set(set("--a=1"), set("--a=3")));
        check(orArgParser.parse("--a=1 (--a=2 | --a=3)"), set(set("--a=1")));
        check(orArgParser.parse("--a=1 (--a=2 --b=1 | --a=3 --b=2)"), set(set("--a=1", "--b=1"), set("--a=1", "--b=2")));
    }

    @Test
    public void commonParameters() {
        var paramLists = orArgParser.parse("--a=(100|1000|10000) --a=100 --b=1").eval().expand();
        var commonParameters = Benchmark.commonParameters(paramLists);
        assertThat(commonParameters, Matchers.is(set("--b=1")));
    }

    private static CmdLineArgsProcessor.Args args(String... args) {
        return CmdLineArgsProcessor.process(args).args;
    }

    public void check(CmdLineArgsProcessor.Args args, Set<Set<String>> expected) {
        // use sets for comparisons
        var actual = paramSets(args.eval());
        assertThat(actual, Matchers.is(expected));
    }

    private static Set<Set<String>> paramSets(CmdLineArgsProcessor.Params params) {
        return params.expand().stream().map(l -> l.stream().collect(Collectors.toSet())).collect(Collectors.toSet());
    }

    private static <T> Set<T> set(T... ts) {
        return Stream.of(ts).collect(Collectors.toSet());
    }
}
