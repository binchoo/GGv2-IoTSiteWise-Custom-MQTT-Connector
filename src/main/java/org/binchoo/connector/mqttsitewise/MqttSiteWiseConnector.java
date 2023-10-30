package org.binchoo.connector.mqttsitewise;


import lombok.Getter;
import org.binchoo.connector.stream.SiteWiseEdgeStream;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.SubscribeToTopicResponseHandler;
import software.amazon.awssdk.aws.greengrass.model.JsonMessage;
import software.amazon.awssdk.aws.greengrass.model.ReceiveMode;
import software.amazon.awssdk.aws.greengrass.model.SubscribeToTopicRequest;
import software.amazon.awssdk.aws.greengrass.model.SubscriptionResponseMessage;
import software.amazon.awssdk.eventstreamrpc.StreamResponseHandler;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

@Getter
@Component
final public class MqttSiteWiseConnector {

    private static final Random rand = new Random();

    private final SiteWiseEdgeStream stream;
    private final ComponentConfiguration input;

    private final GreengrassCoreIPCClientV2 ipcClient;
    private final String topicFilter;
    private final String aliasPattern;


    public MqttSiteWiseConnector(GreengrassCoreIPCClientV2 ipcClient,
                                 SiteWiseEdgeStream stream, ComponentConfiguration input) {
        this.ipcClient = ipcClient;
        this.stream = stream;
        this.input = input;
        this.topicFilter = input.getTopicFilter();
        this.aliasPattern = input.getAliasPattern();
    }

    public SubscribeToTopicResponseHandler run() {
        try {
            SubscribeToTopicRequest subscribeRequest = new SubscribeToTopicRequest()
                    .withTopic(this.topicFilter)
                    .withReceiveMode(ReceiveMode.RECEIVE_MESSAGES_FROM_OTHERS);

            var future = this.ipcClient.subscribeToTopicAsync(subscribeRequest, new StreamResponseHandler<>() {
                @Override
                public void onStreamEvent(SubscriptionResponseMessage streamEvent) {
                    var message = streamEvent.getBinaryMessage();
                    String topic = message.getContext().getTopic();
                    String[] segments = this.segment(topic);

                    String assetName = segments[input.getAssetKey() - 1];
                    String propertyName = segments[input.getPropertyKey() - 1];
                    String propertyAlias = this.synthesisPropertyAlias(aliasPattern, assetName, propertyName);

                    TypedDataMessage payload = TypedDataMessage.of(message.getMessage());
                    stream.sendValue(propertyAlias, payload.getData());
                    System.out.println("Sending a message...");
                }

                @Override
                public boolean onStreamError(Throwable error) {
                    System.err.println("Unable to resolve a received message.");
                    return false;
                }

                @Override
                public void onStreamClosed() {}

                private String[] segment(String wildcardTopic) {
                    if (wildcardTopic.startsWith("/"))
                        wildcardTopic = wildcardTopic.substring(1);
                    return wildcardTopic.split("/");
                }

                private String synthesisPropertyAlias(String aliasPattern, String asset, String property) {
                    return aliasPattern.replace("{asset}", asset)
                            .replace("{property}", property);
                }
            });
            return future.getHandler();

        } catch (Exception ignored){
        }

        return null;
    }
}
