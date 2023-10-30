package org.binchoo.connector.stream;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import com.amazonaws.greengrass.streammanager.model.StrategyOnFull;
import com.amazonaws.greengrass.streammanager.model.export.ExportDefinition;

public class SiteWiseProcessorInputStream extends AbstractSiteWiseEdgeStream {

    private static final String STREAM_NAME = "SiteWise_Edge_Stream";

    public SiteWiseProcessorInputStream(StreamManagerClient client) {
        super(client, STREAM_NAME);
    }

    @Override
    public ExportDefinition getExportDefinition() {
        /* SiteWise_Edge_Stream is managed stream. No specific attribute is required. */
        return null;
    }

    @Override
    public StrategyOnFull getStrategyOnFull() {
        /* SiteWise_Edge_Stream is managed stream. No specific attribute is required. */
        return null;
    }
}
