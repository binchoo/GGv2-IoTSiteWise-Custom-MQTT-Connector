package org.binchoo.connector.mqttsitewise;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import com.amazonaws.greengrass.streammanager.client.StreamManagerClientFactory;
import com.amazonaws.greengrass.streammanager.client.exception.StreamManagerException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.binchoo.connector.stream.EdgeStream;
import org.binchoo.connector.stream.SiteWiseEdgeStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.GetConfigurationRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Configuration
public class Bootstrap {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static final String COMPONENT_NAME = "org.binchoo.connector.mqttsitewise.MqttSiteWiseConnector";
    private static final String COMPONENT_CONFIG_KEY = "configuration";

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public GreengrassCoreIPCClientV2 ipcClient() throws IOException {
        return GreengrassCoreIPCClientV2.builder().build();
    }

    @Bean
    public StreamManagerClient streamManagerClient() {
        try {
            return StreamManagerClientFactory.standard().build();
        } catch (StreamManagerException e) {
            logger.error("Failed to create StreamManagerClient", e);
            throw new RuntimeException(e);
        }
    }

    @Bean
    public ComponentConfig componentConfig(GreengrassCoreIPCClientV2 ipcClient, ObjectMapper objectMapper) {
        try {
            Map<String, Object> json = requestComponentConfiguration(ipcClient);
            return objectMapper.convertValue(json, ComponentConfig.class);
        } catch (Exception e) {
            logger.error("Failed to fetch component configuration", e);
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> requestComponentConfiguration(GreengrassCoreIPCClientV2 ipcClient) throws InterruptedException {
        GetConfigurationRequest request = new GetConfigurationRequest().withComponentName(COMPONENT_NAME)
                .withKeyPath(List.of(COMPONENT_CONFIG_KEY));
        return ipcClient.getConfiguration(request).getValue();
    }

    @Bean
    public SiteWiseEdgeStream sitewiseEdgeStream() {
        return SiteWiseEdgeStream.processorInputStream();
    }

    @Bean
    public MqttSiteWiseConnector mqttSiteWiseConnector(GreengrassCoreIPCClientV2 ipcClient,
                                                       EdgeStream edgeStream,
                                                       ComponentConfig componentConfig) {

        return new MqttSiteWiseConnector(ipcClient, edgeStream, componentConfig);
    }
}
