package org.binchoo.connector.mqttsitewise;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {

    public static void main(String... args) {
        Thread t = startConnector();
        wait(t);
    }

    private static Thread startConnector() {
        ApplicationContext context = new AnnotationConfigApplicationContext(Bootstrap.class);
        MqttSiteWiseConnector connector = context.getBean(MqttSiteWiseConnector.class);

        Thread thread = new Thread(connector) {{
            start();
        }};

        System.out.println("Connector is running");
        return thread;
    }

    private static void wait(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            System.err.println("Connector is interrupted");
            throw new RuntimeException(e);
        }
        System.out.println("Component is closing");
    }
}
