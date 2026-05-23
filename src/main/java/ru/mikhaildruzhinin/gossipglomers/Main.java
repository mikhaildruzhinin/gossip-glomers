package ru.mikhaildruzhinin.gossipglomers;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    private static String nodeId;

    private static int nextMsgId = 0;

    private static final AtomicInteger id = new AtomicInteger(0);
    
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            JsonNode request = mapper.readTree(line);
            JsonNode body = request.get("body");
            JsonNode type = body.get("type");

            switch (type.asString()) {
                case "init" -> {
                    nodeId = body.get("node_id").asString();
                    System.err.println("Initialized node " + nodeId);
                    ObjectNode responseBody = mapper.createObjectNode();
                    responseBody.put("type", "init_ok");
                    reply(request, responseBody);
                }

                case "echo" -> {
                    System.err.println("Echoing " + body);
                    ObjectNode responseBody = mapper.createObjectNode();
                    responseBody.put("type", "echo_ok");
                    responseBody.put("echo", body.get("echo").asString());
                    reply(request, responseBody);
                }

                case "generate" -> {
                    System.err.println("Generating unique id");
                    ObjectNode responseBody = mapper.createObjectNode();
                    responseBody.put("type", "generate_ok");
                    String uniqueId = nodeId + "-" + id.incrementAndGet();
                    responseBody.put("id", uniqueId);
                    reply(request, responseBody);
                }
            }
        }
    }

    private static void reply(JsonNode request, ObjectNode responseBody) {
        nextMsgId++;

        ObjectNode message = mapper.createObjectNode();
        message.put("src", nodeId);
        message.put("dest", request.get("src").asString());

        responseBody.put("msg_id", nextMsgId);
        responseBody.put("in_reply_to", request.get("body").get("msg_id").asInt());

        message.set("body", responseBody);

        System.err.println(message);
        System.out.println(message);
        System.out.flush();
    }
}
