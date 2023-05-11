/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator;

import io.debezium.metadata.ConnectorMetadata;
import io.debezium.metadata.ConnectorMetadataProvider;
import io.debezium.schemagenerator.schema.Schema;
import io.debezium.schemagenerator.schema.SchemaName;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Provider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SchemaGenerator {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SchemaGenerator.class);

    public static void main(String[] args) {
        if (args.length != 5) {
            LOGGER.info("There were " + args.length + " arguments:");
            for (int i = 0; i < args.length; ++i) {
                LOGGER.info( "  Argument #[" + i + "]: " + args[i]);
            }
            throw new IllegalArgumentException("Usage: SchemaGenerator <format-name> <output-directory> <groupDirectoryPerConnector> <filenamePrefix> <filenameSuffix>");
        }

        String formatName = args[0].trim();
        Path outputDirectory = new File(args[1]).toPath();
        boolean groupDirectoryPerConnector = Boolean.parseBoolean(args[2]);
        String filenamePrefix = args[3];
        String filenameSuffix = args[4];

        new SchemaGenerator().run(formatName, outputDirectory, groupDirectoryPerConnector, filenamePrefix, filenameSuffix);
    }

    private void run(String formatName, Path outputDirectory, boolean groupDirectoryPerConnector, String filenamePrefix, String filenameSuffix) {
        List<ConnectorMetadata> allMetadata = getMetadata();

        Schema format = getSchemaFormat(formatName);
        LOGGER.info( "Using schema format: " + format.getDescriptor().getName());

        if (allMetadata.isEmpty()) {
            throw new RuntimeException("No connectors found in classpath. Exiting!");
        }
        for (ConnectorMetadata connectorMetadata : allMetadata) {
            LOGGER.info( "Creating \"" + format.getDescriptor().getName()
                    + "\" schema for connector: "
                    + connectorMetadata.getConnectorDescriptor().getName() + "...");
            JsonSchemaCreatorService jsonSchemaCreatorService = new JsonSchemaCreatorService(connectorMetadata, format.getFieldFilter());
            org.eclipse.microprofile.openapi.models.media.Schema buildConnectorSchema = jsonSchemaCreatorService.buildConnectorSchema();
            String spec = format.getSpec(buildConnectorSchema);

            try {
                String schemaFilename = "";
                if (groupDirectoryPerConnector) {
                    schemaFilename += connectorMetadata.getConnectorDescriptor().getId() + File.separator;
                }
                if (null != filenamePrefix && !filenamePrefix.isEmpty()) {
                    schemaFilename += filenamePrefix;
                }
                schemaFilename += connectorMetadata.getConnectorDescriptor().getId();
                if (null != filenameSuffix && !filenameSuffix.isEmpty()) {
                    schemaFilename += filenameSuffix;
                }
                schemaFilename += ".json";
                Path schemaFilePath = outputDirectory.resolve(schemaFilename);
                schemaFilePath.getParent().toFile().mkdirs();
                Files.write(schemaFilePath, spec.getBytes(StandardCharsets.UTF_8));
            }
            catch (IOException e) {
                throw new RuntimeException("Couldn't write file", e);
            }
        }
    }

    private List<ConnectorMetadata> getMetadata() {
        ServiceLoader<ConnectorMetadataProvider> metadataProviders = ServiceLoader.load(ConnectorMetadataProvider.class);

        List<ConnectorMetadata>  metadataList = new ArrayList<>();
        metadataProviders.forEach(provider -> metadataList.add(provider.getConnectorMetadata()));
        return metadataList;
    }

    /**
     * Returns the {@link Schema} with the given name, specified via the {@link SchemaName} annotation.
     */
    private Schema getSchemaFormat(String formatName) {
        ServiceLoader<Schema> schemaFormats = ServiceLoader.load(Schema.class);
        Iterator<Schema> formatIterator = schemaFormats.iterator();

        while (formatIterator.hasNext()) {
            Schema format = formatIterator.next();
            if (format.getClass().isAnnotationPresent(SchemaName.class)
                    && format.getClass().getAnnotation(SchemaName.class).value().equals(formatName)) {
                return format;
            }
        }

        throw new RuntimeException("No schema formats found!");
    }
}
