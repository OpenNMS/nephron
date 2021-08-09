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

package org.opennms.nephron.testing.flowgen;

import java.util.List;

import org.apache.flink.types.Either;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;

public class LimiterTest {

    /**
     * Holds the state that used to verify that taking checkpoints does not change the rate-limiting behavior of
     * a limiter.
     */
    private static class State {
        // the wall clock that is used by both limiters
        private long currentTimeMillis;
        private long flowsPerSecond;
        // a reference limiter that does not take part in checkpoints
        private Limiter referenceLimiter;
        // either a limiter or its state that is saved in checkpoints and used to restore the limiter
        private Either<Limiter, Long> limiterOrState;

        public State(long currentTimeMillis, long flowsPerSecond) {
            this.currentTimeMillis = currentTimeMillis;
            this.flowsPerSecond = flowsPerSecond;
            referenceLimiter = Limiter.of(flowsPerSecond, () -> this.currentTimeMillis);
            limiterOrState = new Either.Left(Limiter.of(flowsPerSecond, () -> this.currentTimeMillis));
        }

        @Override
        public String toString() {
            return "State.of(" + currentTimeMillis +"," + flowsPerSecond + ')';
        }
    }

    /**
     * Represents actions that are executed in arbitrary sequence on the test state.
     */
    interface Action {
        /**
         * Applies this action on the state.
         *
         * @return Returns <code>true</code> iff executing this action did not show a mismatch between
         * the reference limiter contained in the state and the limiter that takes part in checkpoints.
         */
        boolean exec(State state);
    }

    public static class TickAction implements Action {
        public static TickAction of(long ms) {
            return new TickAction(ms);
        }
        public final long ms;
        public TickAction(long ms) {
            this.ms = ms;
        }
        @Override
        public boolean exec(State state) {
            state.currentTimeMillis += ms;
            return true;
        }

        @Override
        public String toString() {
            return "TickAction.of(" + ms + ')';
        }
    }

    public static class CheckAction implements Action {
        public static CheckAction of(int flowsPerSecond, int incr) {
            return new CheckAction(flowsPerSecond, incr);
        }
        public final int flowsPerSecond, incr;
        public CheckAction(int flowsPerSecond, int incr) {
            this.flowsPerSecond = flowsPerSecond;
            this.incr = incr;
        }
        @Override
        public boolean exec(State state) {
            if (state.limiterOrState.isRight()) {
                // restore limiter from its limiter state
                state.limiterOrState = new Either.Left(Limiter.restore(flowsPerSecond, state.limiterOrState.right(), () -> state.currentTimeMillis));
            }
            var limiter = state.limiterOrState.left();
            return limiter.check(incr) == state.referenceLimiter.check(incr);
        }

        @Override
        public String toString() {
            return "CheckAction.of(" + flowsPerSecond +"," + incr + ')';
        }
    }

    public static class CheckpointAction implements Action {
        public static CheckpointAction of() {
            return new CheckpointAction();
        }
        @Override
        public boolean exec(State state) {
            if (state.limiterOrState.isLeft()) {
                state.limiterOrState = new Either.Right(state.limiterOrState.left().state());
            }
            return true;
        }

        @Override
        public String toString() {
            return "CheckpointAction.of()";
        }
    }

    private static Arbitrary<Action> action(int flowsPerSecond) {
        var tick = Arbitraries.longs().between(1, 10).map(TickAction::new);
        var check = Arbitraries.integers().between(1, 3).map(incr -> new CheckAction(flowsPerSecond, incr));
        var checkpoint = Arbitraries.just(new CheckpointAction());
        return Arbitraries.frequencyOf(
                Tuple.of(1, tick),
                Tuple.of(1, check),
                Tuple.of(1, checkpoint)
        );
    }

    /**
     * Provides an initial state and a list of actions that are applied to the state.
     */
    @Provide
    public Arbitrary<Tuple.Tuple2<State, List<Action>>> stateAndActions() {
        return Arbitraries.integers()
                .between(50, 5000)
                .flatMap(flowsPerSecond -> action(flowsPerSecond).list().map(list -> Tuple.of(new State(1_500_000_000_000l, flowsPerSecond), list)));
    }

    /**
     * Checks that the limiting behavior of a reference limiter that does not take part in checkpoints and the limiter
     * that does match when starting from the given state and applying the given list of actions.
     */
    @Property
    public boolean checkpointsDoNotInfluenceTheRate(
            @ForAll("stateAndActions") Tuple.Tuple2<State, List<Action>> stateAndActions
    ) {
        var state = stateAndActions.get1();
        for (var a: stateAndActions.get2()) {
            var b = a.exec(state);
            if (!b) return b;
        }
        return true;
    }

}
