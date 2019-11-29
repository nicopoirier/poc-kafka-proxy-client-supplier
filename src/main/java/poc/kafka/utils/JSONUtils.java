package poc.kafka.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

@Slf4j
public class JSONUtils {
    private static final JSONUtils INSTANCE = new JSONUtils();
    @Getter
    private final ObjectMapper objectMapper;

    private JSONUtils() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setLenient(false);
        objectMapper.setDateFormat(dateFormat);

    }

    public static JSONUtils getInstance() {
        return INSTANCE;
    }

    public JsonNode parse(String raw) throws IOException {
        try {
            return parseWithError(raw);
        } catch (IOException e) {
            log.error("Error", e);
            throw e;
        }
    }


    public <T> T parseJsonNode(JsonNode n, Class<T> destClass) {
        try {
            return objectMapper.convertValue(n, destClass);
        } catch (Exception e) {
            log.warn(e.getMessage());
            return null;
        }
    }

    public JsonNode parseWithError(String raw) throws IOException {
        return objectMapper.readTree(raw);
    }

    public ObjectNode parseObj(String raw) throws IOException {
        return (ObjectNode) parse(raw);
    }

    public <T> String asJsonString(T object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }

    public <T> T parse(byte[] raw, Class<T> destClass) throws IOException {
        try {
            return objectMapper.readValue(raw, destClass);
        } catch (IOException e) {
            log.error("Error", e);
            throw e;
        }
    }


    public <T> T parse(String raw, Class<T> destClass) throws IOException {
        try {
            return objectMapper.readValue(raw, destClass);
        } catch (IOException e) {
            log.warn(e.getMessage());
            throw e;
        }
    }

    public JsonNode toJsonNode(Object obj) {
        return objectMapper.valueToTree(obj);
    }

    public String asJsonPath(String path) {
        if (StringUtils.isBlank(path)) {
            return null;
        }
        return "/" + StringUtils.replace(path, ".", "/");
    }

    public boolean has(JsonNode jsonNode, String path) {
        String jsonPath = asJsonPath(path);
        JsonNode targetNode = jsonNode.at(jsonPath);
        return targetNode != null && targetNode.getNodeType() != JsonNodeType.NULL && targetNode.getNodeType() != JsonNodeType.MISSING;
    }

    public JsonNode at(JsonNode jsonNode, String path) {
        String jsonPath = asJsonPath(path);
        return jsonNode.at(jsonPath);
    }

    public void put(JsonNode jsonNode, String path, JsonNode value) {
        JsonPointer valueNodePointer = JsonPointer.compile(asJsonPath(path));
        JsonPointer parentPointer = valueNodePointer.head();
        addMissingNodeIfNecessary(jsonNode, path);
        JsonNode parentNode = jsonNode.at(parentPointer);

        if (parentNode.getNodeType() == JsonNodeType.OBJECT) {
            ObjectNode parentNodeToUpdate = (ObjectNode) parentNode;
            parentNodeToUpdate.set(StringUtils.replace(valueNodePointer.last().toString(), "/", ""), value);
        }
    }

    private void addMissingNodeIfNecessary(JsonNode jsonNode, String path) {
        String[] properties = path.split("\\.");
        ObjectNode parent = (ObjectNode) jsonNode;
        for (int i = 0; i < properties.length - 1; i++) {
            String elementName = properties[i];
            JsonNode element = parent.get(elementName);
            if (element == null || element.getNodeType() == JsonNodeType.MISSING || element.getNodeType() == JsonNodeType.NULL) {
                parent.set(elementName, JsonNodeFactory.instance.objectNode());
            }
            parent = (ObjectNode) parent.get(elementName);

        }
    }

    public void remove(JsonNode jsonNode, String path) {
        JsonPointer valueNodePointer = JsonPointer.compile(asJsonPath(path));
        JsonPointer parentPointer = valueNodePointer.head();
        JsonNode parentNode = jsonNode.at(parentPointer);
        if (parentNode.getNodeType() == JsonNodeType.OBJECT) {
            ObjectNode parentNodeToUpdate = (ObjectNode) parentNode;
            parentNodeToUpdate.remove(StringUtils.replace(valueNodePointer.last().toString(), "/", ""));
        }

    }

}

