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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.opennms.nephron.NephronOptions;

/**
 * Defines the distribution of generated flows.
 *
 * Parameters can be grouped into the following areas:
 *
 * <dl>
 *     <dt>Randomness</dt>
 *     <dd>
 *         <ul>
 *             <li>seed</li>
 *             <li>playbackMode</li>
 *             <li>startMs (only use in playback mode)</li>
 *         </ul>
 *     </dd>
 *     <dt>Flow instances</dt>
 *     <dd>
 *         <ul>
 *             <li>fixedWindowsSizeMs (inherited from NephronOptions)</li>
 *             <li>numWindows</li>
 *             <li>flowsPerWindow || flowsPerSecond</li>
 *         </ul>
 *     </dd>
 *     <dt>Flow attributes</dt>
 *     <dd>
 *         <ul>
 *             <li>numExporters, numInterfaces, numApplications, numHosts, numDscps, numEcns</li>
 *             <li>lastSwitchedSigma, flowDurationLambda</li>
 *         </ul>
 *     </dd>
 * </dl>
 */
public interface FlowGenOptions extends NephronOptions {

    @Description("Seed for generating synthetic flows.")
    @Default.Long(123456)
    Long getSeed();
    void setSeed(Long num);

    @Description("Approximate number of flows per window. Overridden in playback mode if flowsPerSecond is specified.")
    @Default.Long(1000)
    Long getFlowsPerWindow();
    void setFlowsPerWindow(Long num);

    @Description("The minimum number of splits the input is split into. A single task may use several splits depending on parallelism.")
    @Default.Integer(1)
    Integer getMinSplits();
    void setMinSplits(Integer num);

    @Description("The maximum number of splits the input is split into. A single task may use several splits depending on parallelism.")
    @Default.Integer(1)
    Integer getMaxSplits();
    void setMaxSplits(Integer num);

    @Description("Approximate number of windows.")
    @Default.Integer(10)
    Integer getNumWindows();
    void setNumWindows(Integer num);

    @Description("The minimum exporter number.")
    @Default.Integer(2)
    Integer getMinExporter();
    void setMinExporter(Integer num);

    @Description("The number of exporters.")
    @Default.Integer(5)
    Integer getNumExporters();
    void setNumExporters(Integer num);

    @Description("Partition exporters into a number of groups each having its own clock skew.")
    @Default.Integer(1)
    Integer getNumClockSkewGroups();
    void setNumClockSkewGroups(Integer num);

    @Description("Clock skew difference between different clock skew groups. Must be a positive number.")
    @Default.Long(10000)
    Long getClockSkewMs();
    void setClockSkewMs(Long num);

    @Description("Determines if skewed clocks are ahead of time, behind of time, or both.")
    @Default.Enum("BOTH")
    ClockSkewDirection getClockSkewDirection();
    void setClockSkewDirection(ClockSkewDirection value);

    @Description("The minimum interface number.")
    @Default.Integer(3)
    Integer getMinInterface();
    void setMinInterface(Integer num);

    @Description("The number of interfaces.")
    @Default.Integer(1)
    Integer getNumInterfaces();
    void setNumInterfaces(Integer num);

    @Description("The number of protocols.")
    @Default.Integer(10)
    Integer getNumProtocols();
    void setNumProtocols(Integer num);

    @Description("The number of applications.")
    @Default.Integer(10)
    Integer getNumApplications();
    void setNumApplications(Integer num);

    @Description("The number of hosts.")
    @Default.Integer(5)
    Integer getNumHosts();
    void setNumHosts(Integer num);

    @Description("The number of ECN values. Values 1 to 4 are allowed.")
    @Default.Integer(4)
    Integer getNumEcns();
    void setNumEcns(Integer num);

    @Description("The number of DSCP values. Values 1 to 64 are allowed.")
    @Default.Integer(17)
    Integer getNumDscps();
    void setNumDscps(Integer num);

    /**
     * The standard derivation of a normal distribution that determines the shift of the end of a flow
     * with regard to an end that would be calculated by a linear time progression.
     * <p>
     * The values less than one standard deviation away from the mean account for 68.27% of the set;
     * while two standard deviations from the mean account for 95.45%; and three standard deviations account for 99.73%.
     */
    @Description("Standard deviation of lastSwitched from its calculated value.")
    @Default.Long(0)
    Long getLastSwitchedSigmaMs();
    void setLastSwitchedSigmaMs(Long millis);

    /**
     * The lambda (decay) factor of an exponential distribution that determines flow durations.
     * <p>
     * The following table shows some probability values for specific flow durations.
     * <table>
     *     <caption>Probabilty of some flow durations</caption>
     *     <tr><th>duration</th><th>l=0.25</th><th>l=0.5</th><th>l=1.0</th></tr>
     *     <tr><td>1</td><td>0.22</td><td>0.39</td><td>0.63</td></tr>
     *     <tr><td>2</td><td>0.39</td><td>0.63</td><td>0.86</td></tr>
     *     <tr><td>3</td><td>0.53</td><td>0.78</td><td>0.95</td></tr>
     *     <tr><td>4</td><td>0.63</td><td>0.86</td><td>0.98</td></tr>
     *     <tr><td>5</td><td>0.71</td><td>0.92</td><td>0.99</td></tr>
     *     <tr><td>6</td><td>0.78</td><td>0.95</td><td>0.997</td></tr>
     *     <tr><td>10</td><td>0.92</td><td>0.99</td><td>0.99999</td></tr>
     *     <tr><td>20</td><td>0.99</td><td>0.99999</td><td>1</td></tr>
     * </table>
     * For example if lambda is 0.5 then 39% of flows have a length shorter than 1 second and 99% of flows have a length
     * that is shorter than 10 seconds..
     */
    @Description("The lambda (decay) factor for the exponential distribution of flow durations (in seconds; larger lambdas yield shorter flow durations).")
    @Default.Double(0.5)
    Double getFlowDurationLambda();
    void setFlowDurationLambda(Double lambda);

    /**
     * In playback mode timestamps are calculated based on the given {@code start} whereas in non-playback mode the
     * current time is used.
     *
     * In non-playback mode either the setting for {@code flowsPerWindow} or {@code flowsPerSecond} is calculated:
     * <ul>
     * <li>If {@code flowsPerSecond} is set then {@code flowsPerWindow} is calculated by {@code flowsPerSec * windowSizeInSec}</li>
     * <li>otherwise {@code flowsPerSecond} is calculated by {@code flowsPerWindow / windowSizeInSec}</li>
     * </ul>
     */
    @Description("In playback mode calculated timestamps are used. In non-playback mode the current time is used. In playback mode flow generation is completely deterministic.")
    @Default.Boolean(true)
    Boolean getPlaybackMode();
    void setPlaybackMode(Boolean value);

    @Description("Start timestamp for generated flows. Only considered in playback mode. In non-playback mode current timestamps are used.")
    @Default.Long(1_500_000_000_000l) // GMT: Friday, July 14, 2017 2:40:00 AM
    Long getStartMs();
    void setStartMs(Long num);

    @Description("Rate limitation for generating flows. Set to non-positive value to disable. If not set in non-playback mode then it is calculated according to flowsPerWindow and windowSize.")
    @Default.Long(0)
    Long getFlowsPerSecond();
    void setFlowsPerSecond(Long value);

    static void adjustNonPlaybackMode(FlowGenOptions options) {
        if (!options.getPlaybackMode()) {
            // in nonplayback mode flowsPerSecond, flowsPerWindow, and fixedWindowSizeMs are interrelated
            // -> make necessary adjustments
            if (options.getFlowsPerSecond() > 0) {
                options.setFlowsPerWindow(options.getFlowsPerSecond() * options.getFixedWindowSizeMs() / 1000);
            } else {
                options.setFlowsPerSecond(options.getFlowsPerWindow() * 1000 / options.getFixedWindowSizeMs());
            }
        }
    }
}
