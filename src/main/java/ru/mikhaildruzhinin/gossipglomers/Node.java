package ru.mikhaildruzhinin.gossipglomers;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Node {

    private String nodeId;

    private final List<String> nodeIds = new ArrayList<>();

    private final Set<String> neighbours = ConcurrentHashMap.newKeySet();

    private static final int FANOUT = 5;

    private final AtomicInteger id = new AtomicInteger(0);

    private final ObjectMapper mapper = new ObjectMapper();

    private final Set<Integer> storedMessages = ConcurrentHashMap.newKeySet();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final Object stdoutLock = new Object();

    private final Object stderrLock = new Object();

    public void run() {
        boolean gossipStarted = false;
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            JsonNode request = mapper.readTree(line);
            JsonNode requestBody = request.get("body");
            if (requestBody == null) {
                throw new RuntimeException("Body is not set");
            }

            JsonNode type = requestBody.get("type");
            if (type == null) {
                throw new RuntimeException("Type is not set");
            }

            Optional<ObjectNode> responseBody = switch (type.asString()) {
                case "init" -> {
                    Optional<ObjectNode> rb = handleInit(requestBody);
                    if (!gossipStarted) {
                        gossipStarted = true;
                        startGossipScheduler();
                    }
                    yield rb;
                }
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
        log(nodeId + ": Receiving gossip " + requestBody);

        ObjectReader reader = mapper.readerFor(new TypeReference<List<Integer>>() {
        });
        List<Integer> messages = reader.readValue(requestBody.get("messages").asArray());

        ArrayList<Integer> newMessages = new ArrayList<>(messages);
        newMessages.removeAll(storedMessages);
        if (!newMessages.isEmpty()) {
            storedMessages.addAll(newMessages);
            gossip(newMessages);
        }
        return Optional.empty();
    }

    private void startGossipScheduler() {
        scheduler.scheduleAtFixedRate(
            this::tryGossip,
            0,
            300,
            TimeUnit.MILLISECONDS
        );
    }

    private void tryGossip() {
        try {
            gossip();
        } catch (Exception e) {
            logError(nodeId + ": Gossip failed: " + e.getMessage(), e);
        }
    }

    private void gossip() {
        if (storedMessages.isEmpty()) {
            return;
        }
        gossip(new ArrayList<>(storedMessages));
    }

    private void gossip(List<Integer> messages) {
        neighbours.forEach( node -> {
            log(nodeId + ": Sending gossip to " + node);

            ObjectNode responseBody = createMessageBody("gossip");
            ArrayNode messagesNode = mapper.createArrayNode();
            messages.forEach(messagesNode::add);
            responseBody.set("messages", messagesNode);
            send(node, responseBody);
        });
    }

    private Optional<ObjectNode> handleTopology() {
        log(nodeId + ": Receiving topology");

        ObjectNode responseBody = mapper.createObjectNode();
        responseBody.put("type", "topology_ok");
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleRead() {
        log(nodeId + ": Reading messages");

        ObjectNode responseBody = createMessageBody("read_ok");
        ArrayNode messages = mapper.createArrayNode();
        storedMessages.forEach(messages::add);
        responseBody.set("messages", messages);
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleBroadcast(JsonNode requestBody) {
        log(nodeId + ": Receiving broadcast " + requestBody);

        int message = requestBody.get("message").asInt();
        boolean isNew = storedMessages.add(message);

        if (isNew) {
            gossip(List.of(message));
        }

        ObjectNode responseBody = createMessageBody("broadcast_ok");
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleGenerate() {
        log(nodeId + ": Generating unique id");

        ObjectNode responseBody = createMessageBody("generate_ok");
        String uniqueId = nodeId + "-" + id.incrementAndGet();
        responseBody.put("id", uniqueId);
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleEcho(JsonNode requestBody) {
        log(nodeId + ": Echoing " + requestBody);

        ObjectNode responseBody = createMessageBody("echo_ok");
        responseBody.put("echo", requestBody.get("echo").asString());
        return Optional.of(responseBody);
    }

    private Optional<ObjectNode> handleInit(JsonNode requestBody) {
        nodeId = requestBody.get("node_id").asString();
        log(nodeId + ": Initialized");
        requestBody.get("node_ids").forEach(node -> nodeIds.add(node.asString()));
        nodeIds.sort(String::compareTo);
        buildTreeNeighbours();

        ObjectNode responseBody = createMessageBody("init_ok");
        return Optional.of(responseBody);
    }

    private void buildTreeNeighbours() {

        int selfIndex = nodeIds.indexOf(nodeId);

        if (selfIndex == -1) {
            throw new RuntimeException("Node id not found: " + nodeId);
        }

        // parent
        if (selfIndex > 0) {
            int parentIndex = (selfIndex - 1) / FANOUT;
            neighbours.add(nodeIds.get(parentIndex));
        }

        // children
        for (int i = 1; i <= FANOUT; i++) {
            int childIndex = selfIndex * FANOUT + i;
            if (childIndex < nodeIds.size()) {
                neighbours.add(nodeIds.get(childIndex));
            }
        }
        log(nodeId + ": Neighbours " + neighbours);
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

        log(message.toString());

        synchronized (stdoutLock) {
            System.out.println(message);
            System.out.flush();
        }
    }

    private void logError(String message, Throwable throwable) {
        synchronized (stderrLock) {
            System.err.println(message);
            throwable.printStackTrace(System.err);
            System.err.flush();
        }
    }

    private void log(String message) {
        synchronized (stderrLock) {
            System.err.println(message);
            System.err.flush();
        }
    }
}
