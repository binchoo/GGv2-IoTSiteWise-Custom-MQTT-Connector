package org.binchoo.connector.mqttsitewise;

import lombok.Getter;
import org.binchoo.connector.stream.SiteWiseEdgeStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.BinaryMessage;
import software.amazon.awssdk.aws.greengrass.model.ReceiveMode;
import software.amazon.awssdk.aws.greengrass.model.SubscribeToTopicRequest;
import software.amazon.awssdk.aws.greengrass.model.SubscriptionResponseMessage;
import software.amazon.awssdk.eventstreamrpc.StreamResponseHandler;

import java.util.Random;

@Getter
@Component
final public class MqttSiteWiseConnector implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MqttSiteWiseConnector.class);
    private static final Random random = new Random();

    private final GreengrassCoreIPCClientV2 ipcClient;
    private final ComponentConfig input;

    private final SiteWiseEdgeStream stream;

    private final String topicFilter;
    private final String aliasPattern;

    public MqttSiteWiseConnector(GreengrassCoreIPCClientV2 ipcClient,
                                 SiteWiseEdgeStream stream, ComponentConfig input) {
        this.ipcClient = ipcClient;
        this.input = input;

        this.stream = stream;

        this.topicFilter = input.getTopicFilter();
        this.aliasPattern = input.getAliasPattern();
    }

    public void run() {
        SubscribeToTopicRequest subscribeRequest = new SubscribeToTopicRequest().withTopic(this.topicFilter)
                    .withReceiveMode(ReceiveMode.RECEIVE_MESSAGES_FROM_OTHERS);

        var future = this.ipcClient.subscribeToTopicAsync(subscribeRequest, new ProtocolResponseHandler());
        var handler = future.getHandler();

        try {
            while (true) {
                Thread.sleep(10000);
                logger.info("MqttSiteWiseConnector status is OK");
            }
        } catch (InterruptedException e) {
            logger.info("MqttSiteWiseConnector is interrupted");
        }

        handler.closeStream();
    }

    private final class ProtocolResponseHandler implements StreamResponseHandler<SubscriptionResponseMessage> {

        @Override
        public void onStreamEvent(SubscriptionResponseMessage streamEvent) {
            BinaryMessage message = streamEvent.getBinaryMessage();

            String topic = message.getContext().getTopic();
            String[] segments = this.segment(topic);

            String assetName = segments[input.getAssetKey() - 1];
            String propertyName = segments[input.getPropertyKey() - 1];
            String propertyAlias = this.synthesisPropertyAlias(aliasPattern, assetName, propertyName);

            TypedDataMessage payload = TypedDataMessage.fromBytes(message.getMessage());
            stream.sendValue(propertyAlias, payload.getData());
        }

        @Override
        public boolean onStreamError(Throwable error) {
            logger.error("Stream encountered an error", error);
            return false;
        }

        @Override
        public void onStreamClosed() {
            logger.info("Stream has been closed");
        }

        private String[] segment(String wildcardTopic) {
            if (wildcardTopic.startsWith("/"))
                wildcardTopic = wildcardTopic.substring(1);
            return wildcardTopic.split("/");
        }

        private String synthesisPropertyAlias(String aliasPattern, String asset, String property) {
            return aliasPattern.replace("{asset}", asset)
                    .replace("{property}", property);
        }
    }
}
