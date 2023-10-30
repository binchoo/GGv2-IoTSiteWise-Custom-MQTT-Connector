package org.binchoo.connector.mqttsitewise;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import software.amazon.awssdk.aws.greengrass.SubscribeToTopicResponseHandler;

public class Main {

    public static void main(String... args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(Bootstrap.class);
        MqttSiteWiseConnector connector = context.getBean(MqttSiteWiseConnector.class);

        SubscribeToTopicResponseHandler subscriptionHandler = connector.run();

        if (subscriptionHandler != null) {
            System.out.println("Successfully subscribed to topic: " + connector.getTopicFilter());

            try {
                while (true) {
                    System.out.println("[system status] OK");
                    Thread.sleep(10000);
                }
            } catch (InterruptedException e) {
                System.err.println("Subscription is interrupted.");
            }
            subscriptionHandler.closeStream();
        }

        System.out.println("Application is closing.");
    }
}
