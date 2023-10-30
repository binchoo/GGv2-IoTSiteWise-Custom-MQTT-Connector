package org.binchoo.connector.stream;

public interface SiteWiseEdgeStream {

    void sendValue(String propertyAlias, Object value);

    void sendValue(String propertyAlias, Object value, Long timestamp);
}
