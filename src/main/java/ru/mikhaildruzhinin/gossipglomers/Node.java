package ru.mikhaildruzhinin.gossipglomers;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {

    private String nodeId;

    private final Set<String> nodeIds = new HashSet<>();

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
                case "gossip" -> handleGossip(requestBody);
                default -> throw new RuntimeException("Unknown type: " + type);
            };
            responseBody.ifPresent(rb -> reply(request, rb));
        }
    }

    private Optional<ObjectNode> handleGossip(JsonNode requestBody) {
        System.err.println(nodeId + ": Receiving gossip " + requestBody);
        System.err.flush();

        int message = requestBody.get("message").asInt();
        boolean isNew = storedMessages.add(message);
        if (isNew) {
            gossip(message);
        }
        return Optional.empty();
    }

    private void gossip(int message) {
        nodeIds.forEach(node -> {
            ObjectNode broadcastBody = createMessageBody("gossip");
            broadcastBody.put("message", message);
            send(node, broadcastBody);
        });
    }

    private Optional<ObjectNode> handleTopology() {
        System.err.println(nodeId + ": Receiving topology");
        System.err.flush();

        ObjectNode responseBody = mapper.createObjectNode();
        responseBody.put("type", "topology_ok");
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleRead() {
        System.err.println(nodeId + ": Reading messages");
        System.err.flush();

        ObjectNode responseBody = createMessageBody("read_ok");
        ArrayNode messages = mapper.createArrayNode();
        storedMessages.forEach(messages::add);
        responseBody.set("messages", messages);
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleBroadcast(JsonNode requestBody) {
        System.err.println(nodeId + ": Receiving broadcast " + requestBody);
        System.err.flush();

        int message = requestBody.get("message").asInt();
        boolean isNew = storedMessages.add(message);
        if (isNew) {
            gossip(message);
        }

        ObjectNode responseBody = createMessageBody("broadcast_ok");
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleGenerate() {
        System.err.println(nodeId + ": Generating unique id");
        System.err.flush();

        ObjectNode responseBody = createMessageBody("generate_ok");
        String uniqueId = nodeId + "-" + id.incrementAndGet();
        responseBody.put("id", uniqueId);
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleEcho(JsonNode requestBody) {
        System.err.println(nodeId + ": Echoing " + requestBody);
        System.err.flush();

        ObjectNode responseBody = createMessageBody("echo_ok");
        responseBody.put("echo", requestBody.get("echo").asString());
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleInit(JsonNode requestBody) {
        nodeId = requestBody.get("node_id").asString();
        System.err.println(nodeId + ": Initialized");
        System.err.flush();
        requestBody.get("node_ids").forEach(node -> {
            String n = node.asString();
            if (!Objects.equals(n, nodeId)) {
                nodeIds.add(n);
            }
        });

        ObjectNode responseBody = createMessageBody("init_ok");
        return Optional.of(responseBody);
    }

    private ObjectNode createMessageBody(String type) {
        ObjectNode responseBody = mapper.createObjectNode();
        responseBody.put("type", type);
        return responseBody;
    }

    private void reply(JsonNode request, ObjectNode responseBody) {
        responseBody.put("in_reply_to", request.get("body").get("msg_id").asInt());
        String dest = request.get("src").asString();
        send(dest, responseBody);
    }

    private void send(String dest, ObjectNode responseBody) {
        ObjectNode message = mapper.createObjectNode();
        message.put("src", nodeId);
        message.put("dest", dest);

        message.set("body", responseBody);

        System.err.println(message);
        System.err.flush();

        System.out.println(message);
        System.out.flush();
    }
}
