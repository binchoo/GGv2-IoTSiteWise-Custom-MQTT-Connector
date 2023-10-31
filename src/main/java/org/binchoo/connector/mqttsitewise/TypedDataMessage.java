package org.binchoo.connector.mqttsitewise;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.binchoo.connector.stream.EdgeStream;

import java.io.IOException;
import java.io.Serializable;

public class TypedDataMessage implements Serializable {

    private static final ObjectMapper mapper = new ObjectMapper();

    private String data;
    private String dataType;
    private Long timestamp;

    public void sendTo(String propertyAlias, EdgeStream via) {
        if ("integer".equals(dataType) || "int".equals(dataType))
            via.sendValue(propertyAlias, Long.parseLong(data));
        else if ("double".equals(dataType) || "float".equals(dataType))
            via.sendValue(propertyAlias, Double.parseDouble(data));
        else if ("string".equals(dataType) || "str".equals(dataType))
            via.sendValue(propertyAlias, data);
        else if ("boolean".equals(dataType) || "bool".equals(dataType))
            via.sendValue(propertyAlias, Boolean.parseBoolean(data));
        else
            throw new RuntimeException(String.format("Invalid data type %s", dataType));
    }

    public String getData() {
        return this.data;
    }

    public String getDataType() {
        return this.dataType;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public static TypedDataMessage fromBytes(byte[] message) {
        try {
            return mapper.readValue(message, TypedDataMessage.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
