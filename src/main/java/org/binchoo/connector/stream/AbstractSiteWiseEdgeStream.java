package org.binchoo.connector.stream;

import com.amazonaws.greengrass.streammanager.model.sitewise.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class AbstractSiteWiseEdgeStream implements SiteWiseEdgeStream {

    protected PutAssetPropertyValueEntry assetValueEntryOf(String propertyAlias, Object value) {
        TimeInNanos timeInNanos = new TimeInNanos()
                .withTimeInSeconds(Instant.now().getEpochSecond());

        List<AssetPropertyValue> entries = new ArrayList<>() {{
            AssetPropertyValue propertyValue = new AssetPropertyValue()
                    .withQuality(Quality.GOOD)
                    .withTimestamp(timeInNanos);

            if (value instanceof Long) {
                propertyValue.setValue(new Variant().withIntegerValue((Long) value));
            } else if (value instanceof Double) {
                propertyValue.setValue(new Variant().withDoubleValue((Double) value));
            } else if (value instanceof Boolean) {
                propertyValue.setValue(new Variant().withBooleanValue((Boolean) value));
            } else if (value instanceof String) {
                propertyValue.setValue(new Variant().withStringValue((String) value));
            }

            add(propertyValue);
        }};

        return new PutAssetPropertyValueEntry()
                .withEntryId(UUID.randomUUID().toString())
                .withPropertyAlias(propertyAlias)
                .withPropertyValues(entries);
    }
}
