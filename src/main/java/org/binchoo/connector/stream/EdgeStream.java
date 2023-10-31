package org.binchoo.connector.stream;

import com.amazonaws.greengrass.streammanager.model.sitewise.PutAssetPropertyValueEntry;

public interface EdgeStream {

    void sendValue(String propertyAlias, PutAssetPropertyValueEntry value);

    void sendValue(String propertyAlias, double value);
    void sendValue(String propertyAlias, double value, long timestamp);

    void sendValue(String propertyAlias, long value);
    void sendValue(String propertyAlias, long value, long timestamp);

    void sendValue(String propertyAlias, String value);
    void sendValue(String propertyAlias, String value, long timestamp);

    void sendValue(String propertyAlias, boolean value);
    void sendValue(String propertyAlias, boolean value, long timestamp);
}
