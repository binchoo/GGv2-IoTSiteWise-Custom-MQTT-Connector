package org.binchoo.connector.stream;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import com.amazonaws.greengrass.streammanager.client.StreamManagerClientFactory;
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

public abstract class SiteWiseEdgeStream implements EdgeStream {

    private static final Logger logger = LoggerFactory.getLogger(SiteWiseEdgeStream.class);
    private static final String SITEWISE_PROCESSOR_STREAM_NAME = "SiteWise_Edge_Stream";
    private static final String SITEWISE_PUBLISHER_STREAM_NAME = "SiteWise_Stream";
    private static final SiteWiseEdgeStream siteWiseProcessorInputStream = new SiteWiseEdgeStream(SITEWISE_PROCESSOR_STREAM_NAME) {
        @Override
        public ExportDefinition getExportDefinition() { return null; }
        @Override
        public StrategyOnFull getStrategyOnFull() { return null; }
    };

    private final StreamManagerClient client;
    private final String streamName;

    public static SiteWiseEdgeStream processorInputStream() {
        return SiteWiseEdgeStream.siteWiseProcessorInputStream;
    }

    protected SiteWiseEdgeStream(String streamName) {
        try {
            this.client = StreamManagerClientFactory.standard().build();
        } catch (StreamManagerException e) {
            logger.error("Failed to initialize StreamManager client");
            throw new RuntimeException(e);
        }
        this.streamName = streamName;
        this.createStream();
    }

    public void createStream() {
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

    public void deleteStream() {
        if (!this.isManagedStream()) {
            try {
                this.client.deleteMessageStream(this.streamName);
                this.client.close();
                logger.info("Stream is deleted: {}", this);
            } catch (StreamManagerException e) {
                logger.warn("Failed to delete a stream: {}", this);
            } catch (Exception e) {
                throw new RuntimeException(e);
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

    public void sendValue(String propertyAlias, PutAssetPropertyValueEntry entry) {
        try {
            byte[] jsonBytes = ValidateAndSerialize.validateAndSerializeToJsonBytes(entry);
            logger.info("Sending a message {} to {}", entry, propertyAlias);
            this.client.appendMessage(this.streamName, jsonBytes);
        } catch (StreamManagerException e) {
            logger.error("Failed to send a message to {}", propertyAlias);
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            logger.error("The message {} was not JSON serializable.", entry);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sendValue(String propertyAlias, double value) {
        this.sendValue(propertyAlias, value, epochSeconds());
    }

    @Override
    public void sendValue(String propertyAlias, long value) {
        this.sendValue(propertyAlias, value, epochSeconds());
    }

    @Override
    public void sendValue(String propertyAlias, String value) {
        this.sendValue(propertyAlias, value, epochSeconds());
    }

    @Override
    public void sendValue(String propertyAlias, boolean value) {
        this.sendValue(propertyAlias, value, epochSeconds());
    }

    private Long epochSeconds() {
        return Instant.now().getEpochSecond();
    }

    @Override
    public void sendValue(String propertyAlias, double value, long timestamp) {
        PutAssetPropertyValueEntry entry = valueEntryOf(propertyAlias, new Variant().withDoubleValue(value), timestamp);
        this.sendValue(propertyAlias, entry);
    }

    @Override
    public void sendValue(String propertyAlias, long value, long timestamp) {
        PutAssetPropertyValueEntry entry = valueEntryOf(propertyAlias, new Variant().withIntegerValue(value), timestamp);
        this.sendValue(propertyAlias, entry);
    }

    @Override
    public void sendValue(String propertyAlias, String value, long timestamp) {
        PutAssetPropertyValueEntry entry = valueEntryOf(propertyAlias, new Variant().withStringValue(value), timestamp);
        this.sendValue(propertyAlias, entry);
    }

    @Override
    public void sendValue(String propertyAlias, boolean value, long timestamp) {
        PutAssetPropertyValueEntry entry = valueEntryOf(propertyAlias, new Variant().withBooleanValue(value), timestamp);
        this.sendValue(propertyAlias, entry);
    }

    protected PutAssetPropertyValueEntry valueEntryOf(String propertyAlias, Variant variant, long timestamp) {
        List<AssetPropertyValue> entries = new ArrayList<>() {{
            AssetPropertyValue propertyValue = new AssetPropertyValue()
                    .withTimestamp(new TimeInNanos().withTimeInSeconds(timestamp))
                    .withQuality(Quality.GOOD)
                    .withValue(variant);
            add(propertyValue);
        }};

        return new PutAssetPropertyValueEntry().withPropertyAlias(propertyAlias)
                .withEntryId(UUID.randomUUID().toString())
                .withPropertyValues(entries);
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
