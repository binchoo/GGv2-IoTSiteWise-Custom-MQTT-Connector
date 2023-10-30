package org.binchoo.connector.mqttsitewise;

import com.amazonaws.greengrass.streammanager.client.StreamManagerClient;
import com.amazonaws.greengrass.streammanager.client.StreamManagerClientFactory;
import com.amazonaws.greengrass.streammanager.client.exception.StreamManagerException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.binchoo.connector.stream.BasicSiteWiseEdgeStream;
import org.binchoo.connector.stream.SiteWiseEdgeStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.GetConfigurationRequest;
import software.amazon.awssdk.aws.greengrass.model.GetConfigurationResponse;

import java.io.IOException;
import java.util.List;

@Configuration
public class Bootstrap {

    private static final String COMPONENT_NAME = "org.binchoo.connector.mqttsitewise.MqttSiteWiseConnector";

    @Bean
    public StreamManagerClient streamManagerClient() {
        try {
            return StreamManagerClientFactory.standard().build();
        } catch (StreamManagerException e) {
            System.err.println("Failed to create StreamManagerClient");
            throw new RuntimeException(e);
        }
    }

    @Bean
    public ComponentConfiguration configInput() {
        try (GreengrassCoreIPCClientV2 ipcClient = GreengrassCoreIPCClientV2.builder().build()) {
            GetConfigurationRequest request = new GetConfigurationRequest().withComponentName(COMPONENT_NAME)
                    .withKeyPath(List.of("configuration"));
            GetConfigurationResponse response = ipcClient.getConfiguration(request);
            return new ObjectMapper().convertValue(response.getValue(), ComponentConfiguration.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    public SiteWiseEdgeStream sitewiseEdgeStream() {
        return new BasicSiteWiseEdgeStream(streamManagerClient());
    }

    @Bean
    public GreengrassCoreIPCClientV2 ipcClientV2() throws IOException {
        return GreengrassCoreIPCClientV2.builder().build();
    }

    @Bean
    public MqttSiteWiseConnector mqttSiteWiseConnector() throws IOException {
        return new MqttSiteWiseConnector(ipcClientV2(), sitewiseEdgeStream(), configInput());
    }
}
