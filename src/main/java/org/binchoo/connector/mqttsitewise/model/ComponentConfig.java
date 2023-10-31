package org.binchoo.connector.mqttsitewise.model;

import lombok.Data;

@Data
public class ComponentConfig {

    /**
     * Example: "/clients/mqtt/iotsitewise/+/+"
     */
    private String topicFilter;

    /**
     * Example: 4
     */
    private Integer assetKey;

    /**
     * Example: 5
     */
    private Integer propertyKey;

    /**
     * Example: "/EstrellaWinds/xandar/hvac-model/{asset}/{property}"
     */
    private String aliasPattern;
}
