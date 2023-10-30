package org.binchoo.connector.stream;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import com.amazonaws.greengrass.streammanager.client.exception.StreamManagerException;
import com.amazonaws.greengrass.streammanager.client.utils.ValidateAndSerialize;
import com.amazonaws.greengrass.streammanager.model.MessageStreamDefinition;
import com.amazonaws.greengrass.streammanager.model.StrategyOnFull;
import com.amazonaws.greengrass.streammanager.model.export.ExportDefinition;
import com.amazonaws.greengrass.streammanager.model.sitewise.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class AbstractSiteWiseEdgeStream implements SiteWiseEdgeStream {

    private static final String SITEWISE_PROCESSOR_STREAM_NAME = "SiteWise_Edge_Stream";
    private static final String SITEWISE_PUBLISHER_STREAM_NAME = "SiteWise_Stream";
    private static final Logger logger = LoggerFactory.getLogger(AbstractSiteWiseEdgeStream.class);

    private final StreamManagerClient client;
    private final String streamName;

    protected AbstractSiteWiseEdgeStream(StreamManagerClient client, String streamName) {
        this.client = client;
        this.streamName = streamName;

        this.createStream();
    }

    protected void createStream() {
        if (!this.isManagedStream()) {
            this.deleteStream();

            MessageStreamDefinition streamDefinition = new MessageStreamDefinition().withName(this.streamName)
                    .withStrategyOnFull(this.getStrategyOnFull())
                    .withExportDefinition(this.getExportDefinition());

            try {
                this.client.createMessageStream(streamDefinition);
                logger.info("Newly created a stream: {}", this);
            } catch (StreamManagerException e) {
                logger.error("Failed to create a stream: {}", this);
                throw new RuntimeException(e);
            }
        }
        else {
            logger.info("Skip creating stream: {}", this);
        }
    }

    protected void deleteStream() {
        if (!this.isManagedStream()) {
            try {
                this.client.deleteMessageStream(this.streamName);
                logger.info("Stream is deleted: {}", this);
            } catch (StreamManagerException e) {
                logger.warn("Error: ", e);
                logger.warn("Failed to delete a stream: {}", this);
            }
        }
        else {
            logger.info("Managed stream cannot be deleted: {}", this);
        }
    }

    private boolean isManagedStream() {
        return SITEWISE_PROCESSOR_STREAM_NAME.equals(this.streamName)
                || SITEWISE_PUBLISHER_STREAM_NAME.equals(this.streamName);
    }

    protected PutAssetPropertyValueEntry assetValueEntryOf(String propertyAlias, Object value, Long timestamp) {
        TimeInNanos timeInNanos = new TimeInNanos().withTimeInSeconds(timestamp);

        List<AssetPropertyValue> entries = new ArrayList<>() {{
            AssetPropertyValue propertyValue = new AssetPropertyValue()
                    .withQuality(Quality.GOOD)
                    .withTimestamp(timeInNanos);

            if (value instanceof Long) {
                propertyValue.setValue(new Variant().withIntegerValue((Long) value));
            } else if (value instanceof Double) {
                propertyValue.setValue(new Variant().withDoubleValue((Double) value));
            } else if (value instanceof Boolean) {
                propertyValue.setValue(new Variant().withBooleanValue((Boolean) value));
            } else if (value instanceof String) {
                propertyValue.setValue(new Variant().withStringValue((String) value));
            }

            add(propertyValue);
        }};

        return new PutAssetPropertyValueEntry().withPropertyAlias(propertyAlias)
                .withPropertyValues(entries)
                .withEntryId(UUID.randomUUID().toString());
    }

    @Override
    public void sendValue(String propertyAlias, Object value, Long timestamp) {
        try {
            PutAssetPropertyValueEntry entry = assetValueEntryOf(propertyAlias, value, timestamp);
            byte[] jsonBytes = ValidateAndSerialize.validateAndSerializeToJsonBytes(entry);
            logger.info("Sending a message {} to {}", entry, propertyAlias);
            this.client.appendMessage(this.streamName, jsonBytes);
        } catch (StreamManagerException e) {
            logger.error("Failed to send a message to {}", propertyAlias);
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            logger.error("The message {} was not JSON serializable.", value.toString());
            throw new RuntimeException(e);
        }
    }
    @Override
    public void sendValue(String propertyAlias, Object value) {
        this.sendValue(propertyAlias, value, Instant.now().getEpochSecond());
    }

    public abstract ExportDefinition getExportDefinition();

    public abstract StrategyOnFull getStrategyOnFull();

    @Override
    public String toString() {
        return "SiteWiseEdgeStream{" +
                "streamName='" + streamName + '\'' +
                '}';
    }
}
