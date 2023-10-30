package org.binchoo.connector.stream;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import com.amazonaws.greengrass.streammanager.client.exception.StreamManagerException;
import com.amazonaws.greengrass.streammanager.client.utils.ValidateAndSerialize;
import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import com.amazonaws.greengrass.streammanager.model.StrategyOnFull;
import com.amazonaws.greengrass.streammanager.model.export.ExportDefinition;
import com.amazonaws.greengrass.streammanager.model.export.IoTSiteWiseConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.UUID;

public class BasicSiteWiseEdgeStream extends AbstractSiteWiseEdgeStream {

    private static final String STREAM_NAME = "SiteWise_Edge_Stream";
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicSiteWiseEdgeStream.class);

    private final StreamManagerClient client;

    public BasicSiteWiseEdgeStream(StreamManagerClient client) {
        this.client = client;
//        this.createStream(STREAM_NAME);
    }

    private void createStream(String streamName) {
        try {
            this.client.deleteMessageStream(streamName);
        } catch (StreamManagerException ignored) {
        }

        ExportDefinition exportDefinition = new ExportDefinition()
                .withIotSitewise(new ArrayList<>() {{
                    add(new IoTSiteWiseConfig()
                            .withIdentifier(UUID.randomUUID().toString())
                            .withBatchSize(5L));
                }});

        MessageStreamDefinition messageStreamDefinition = new MessageStreamDefinition()
                .withName(streamName)
                .withStrategyOnFull(StrategyOnFull.OverwriteOldestData)
                .withExportDefinition(exportDefinition);

        try {
            this.client.createMessageStream(messageStreamDefinition);
            LOGGER.info("Successfully created a stream {}", STREAM_NAME);
        } catch (StreamManagerException e) {
            LOGGER.error("Failed to create a message stream {}", STREAM_NAME);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sendValue(String propertyAlias, Object value) {
        try {
            byte[] message = ValidateAndSerialize.validateAndSerializeToJsonBytes(assetValueEntryOf(propertyAlias, value));
            this.client.appendMessage(STREAM_NAME, message);
            LOGGER.info("Sending a message {} to {}", new String(message, StandardCharsets.UTF_8), STREAM_NAME);
        } catch (StreamManagerException e) {
            LOGGER.error("Failed to send a message to {}", propertyAlias);
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            LOGGER.error("Message {} is not JSON serializable.", value.toString());
            throw new RuntimeException(e);
        }
    }
}
