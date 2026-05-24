package ru.mikhaildruzhinin.gossipglomers;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.HashSet;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {

    private String nodeId;

    private final AtomicInteger id = new AtomicInteger(0);

    private final ObjectMapper mapper = new ObjectMapper();

    private final Set<Integer> storedMessages = new HashSet<>();

    public void run() {
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            JsonNode request = mapper.readTree(line);
            JsonNode requestBody = request.get("body");
            JsonNode type = request.get("body").get("type");

            if (type == null) {
                throw new RuntimeException("Type is not set");
            }

            Optional<ObjectNode> responseBody = switch (type.asString()) {
                case "init" -> handleInit(requestBody);
                case "echo" -> handleEcho(requestBody);
                case "generate" -> handleGenerate();
                case "broadcast" -> handleBroadcast(requestBody);
                case "read" -> handleRead();
                case "topology" -> handleTopology();
                default -> throw new RuntimeException("Unknown type: " + type);
            };
            responseBody.ifPresent(rb -> reply(request, rb));
        }
    }

    private Optional<ObjectNode> handleTopology() {
        System.err.println("Topology");
        System.err.flush();

        ObjectNode responseBody = mapper.createObjectNode();
        responseBody.put("type", "topology_ok");
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleRead() {
        System.err.println("Reading messages");
        System.err.flush();

        ObjectNode responseBody = createResponseBody("read_ok");
        ArrayNode messages = mapper.createArrayNode();
        storedMessages.forEach(messages::add);
        responseBody.set("messages", messages);
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleBroadcast(JsonNode requestBody) {
        System.err.println("Broadcasting " + requestBody);
        System.err.flush();

        storedMessages.add(requestBody.get("message").asInt());
        ObjectNode responseBody = createResponseBody("broadcast_ok");
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleGenerate() {
        System.err.println("Generating unique id");
        System.err.flush();

        ObjectNode responseBody = createResponseBody("generate_ok");
        String uniqueId = nodeId + "-" + id.incrementAndGet();
        responseBody.put("id", uniqueId);
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleEcho(JsonNode requestBody) {
        System.err.println("Echoing " + requestBody);
        System.err.flush();

        ObjectNode responseBody = createResponseBody("echo_ok");
        responseBody.put("echo", requestBody.get("echo").asString());
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleInit(JsonNode requestBody) {
        nodeId = requestBody.get("node_id").asString();
        System.err.println("Initialized node " + nodeId);
        System.err.flush();

        ObjectNode responseBody = createResponseBody("init_ok");
        return Optional.of(responseBody);
    }

    private ObjectNode createResponseBody(String type) {
        ObjectNode responseBody = mapper.createObjectNode();
        responseBody.put("type", type);
        return responseBody;
    }

    private void reply(JsonNode request, ObjectNode responseBody) {
        ObjectNode message = mapper.createObjectNode();
        message.put("src", nodeId);
        message.put("dest", request.get("src").asString());

        responseBody.put("in_reply_to", request.get("body").get("msg_id").asInt());

        message.set("body", responseBody);

        System.err.println(message);
        System.err.flush();
        System.out.println(message);
        System.out.flush();
    }
}
