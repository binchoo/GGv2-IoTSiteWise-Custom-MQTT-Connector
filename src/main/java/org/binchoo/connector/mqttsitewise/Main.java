package org.binchoo.connector.mqttsitewise;

import org.binchoo.connector.mqttsitewise.config.Bootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String... args) {
        keep(startConnector());
    }

    private static Thread startConnector() {
        ApplicationContext context = new AnnotationConfigApplicationContext(Bootstrap.class);
        MqttSiteWiseConnector connector = context.getBean(MqttSiteWiseConnector.class);

        Thread thread = new Thread(connector) {{
            start();
        }};

        logger.info("Connector is running");
        return thread;
    }

    private static void keep(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            logger.warn("Connector is interrupted");
            throw new RuntimeException(e);
        }
        logger.info("Component is closing");
    }
}
