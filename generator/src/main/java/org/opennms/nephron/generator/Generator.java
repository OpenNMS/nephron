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

package org.opennms.nephron.generator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.opennms.nephron.catheter.Exporter;
import org.opennms.nephron.catheter.FlowGenerator;
import org.opennms.nephron.catheter.Simulation;
import org.opennms.nephron.catheter.json.ExporterJson;
import org.opennms.nephron.catheter.json.SimulationJson;

public class Generator implements Cloneable {

    @Argument
    private File jsonConfigFile;

    private void run(final String... args) throws IOException, JAXBException {
        final CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);

            if (this.jsonConfigFile == null) {
                throw new CmdLineException(parser, "No argument is given");
            }
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.err.println("java -jar catheter-1.0-SNAPSHOT-jar-with-dependencies.jar JSON-file");
            parser.printUsage(System.err);
            System.err.println();

            return;
        }

        final Simulation simulation = Generator.fromFile(this.jsonConfigFile);
        simulation.start();
    }

    public static void main(final String... args) throws Exception {
        new Generator().run(args);
    }

    public static Simulation fromFile(final File file) throws JAXBException, FileNotFoundException {
        return fromSource(new StreamSource(new FileReader(file)));
    }

    public static Simulation fromJson(final String json) throws JAXBException {
        return fromSource(new StreamSource(new StringReader(json)));
    }

    private static Simulation fromSource(final Source source) throws JAXBException {
        final Unmarshaller unmarshaller = JAXBContext.newInstance(SimulationJson.class).createUnmarshaller();
        unmarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, "application/json");

        final SimulationJson simulationJson = unmarshaller.unmarshal(source, SimulationJson.class).getValue();

        final List<Exporter.Builder> exporterBuilders = new ArrayList<>();

        for(final ExporterJson exporterJson : simulationJson.getExporters()) {
            final FlowGenerator.Builder flowGeneratorBuilder = FlowGenerator.builder()
                                                                            .withMaxFlowCount(exporterJson.getFlowGenerator().getMaxFlowCount())
                                                                            .withMinFlowDuration(Duration.ofMillis(exporterJson.getFlowGenerator().getMinFlowDurationMs()))
                                                                            .withMaxFlowDuration(Duration.ofMillis(exporterJson.getFlowGenerator().getMaxFlowDurationMs()))
                                                                            .withActiveTimeout(Duration.ofMillis(exporterJson.getFlowGenerator().getActiveTimeoutMs()))
                                                                            .withBytesPerSecond(exporterJson.getFlowGenerator().getBytesPerSecond());

            exporterBuilders.add(Exporter.builder()
                                         .withForeignId(exporterJson.getForeignId())
                                         .withForeignSource(exporterJson.getForeignSource())
                                         .withNodeId(exporterJson.getNodeId())
                                         .withLocation(exporterJson.getLocation())
                                         .withInputSnmp(exporterJson.getInputSnmp())
                                         .withOutputSnmp(exporterJson.getOutputSnmp())
                                         .withGenerator(flowGeneratorBuilder)
                                         .withClockOffset(Duration.ofMillis(exporterJson.getClockOffsetMs())));
        }

        final Handler handler = new Handler(simulationJson.getBootstrapServers(), simulationJson.getFlowTopic(), new Random(simulationJson.getSeed()));

        return Simulation.builder(handler)
                         .withStartTime(simulationJson.getStartTime())
                         .withSeed(simulationJson.getSeed())
                         .withTickMs(Duration.ofMillis(simulationJson.getTickMs()))
                         .withRealtime(simulationJson.getRealtime())
                         .withExporters(exporterBuilders).build();
    }
}
