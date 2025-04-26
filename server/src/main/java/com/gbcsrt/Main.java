package com.gbcsrt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main implements Runnable {
    private static final String TASK_QUEUE_NAME = "msg_queue";
    private static final String EXCHANGE_NAME = "update_exchange";
    private final String jsonFile; // Unique JSON file for each thread
    private final Map<String, Object> data = new HashMap<>(); // Data specific to this thread
    private final ObjectMapper objectMapper = new ObjectMapper(); // ObjectMapper specific to this thread
    private final int threadId;

    public Main(int threadId) {
        this.threadId = threadId;
        this.jsonFile = "data_thread_" + threadId + ".json"; // Assign a unique JSON file to this thread
    }

    @Override
    public void run() {
        try {
            // Load data from the thread-specific JSON file
            loadDataFromFile();

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            try (Connection connection = factory.newConnection();
                 Channel taskChannel = connection.createChannel();
                 Channel updateChannel = connection.createChannel()) {

                // Declare the task queue
                taskChannel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

                // Declare the fanout exchange for updates
                updateChannel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

                // Declare a unique queue for this thread to listen to updates
                String updateQueue = updateChannel.queueDeclare().getQueue();
                updateChannel.queueBind(updateQueue, EXCHANGE_NAME, "");

                System.out.println("[Thread " + threadId + "] Server is waiting for messages...");

                // Task queue consumer
                DeliverCallback taskCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    String correlationId = delivery.getProperties().getCorrelationId();
                    String replyTo = delivery.getProperties().getReplyTo();

                    // Log the received message
                    System.out.println("[Thread " + threadId + "] Received task message: " + message);

                    // Process the message and generate a response
                    String response = processMessage(message);

                    // Log the reply details
                    System.out.println("[Thread " + threadId + "] Replying to queue \"" + replyTo + "\" the message: " + response);

                    // Send the response back to the broker
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                            .correlationId(correlationId)
                            .build();
                    taskChannel.basicPublish("", replyTo, replyProps, response.getBytes(StandardCharsets.UTF_8));
                };

                // Update queue consumer
                DeliverCallback updateCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

                    // Log the received update
                    System.out.println("[Thread " + threadId + "] Received update: " + message);

                    // Process the update
                    processUpdate(message);
                };

                // Start consuming messages from the task queue
                taskChannel.basicConsume(TASK_QUEUE_NAME, true, taskCallback, consumerTag -> {});

                // Start consuming messages from the update queue
                updateChannel.basicConsume(updateQueue, true, updateCallback, consumerTag -> {});

                // Keep the thread alive to continue consuming messages
                synchronized (this) {
                    this.wait(); // Wait indefinitely to keep the thread alive
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String processMessage(String message) {
        try {
            Map<String, Object> requestData = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
            String operation = (String) requestData.get("operation");

            if ("create_post".equals(operation)) {
                Map<String, Object> post = objectMapper.convertValue(requestData.get("data"), new TypeReference<Map<String, Object>>() {});
                String postId = "post_" + (((List<?>) data.get("posts")).size() + 1);
                post.put("postId", postId);
                Object postsObject = data.get("posts");
                if (postsObject instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> posts = (List<Map<String, Object>>) postsObject;
                    posts.add(post);
                } else {
                    throw new IllegalStateException("\"posts\" is not a valid List<Map<String, Object>>");
                }

                // Update the JSON file
                updateJsonFile(post, "add");

                return "{\"status\": \"success\", \"postId\": \"" + postId + "\"}";
            } else if ("get_posts".equals(operation)) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> posts = (List<Map<String, Object>>) data.get("posts");
                return objectMapper.writeValueAsString(posts);
            } else {
                return "{\"status\": \"error\", \"message\": \"Unknown operation\"}";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"status\": \"error\", \"message\": \"Failed to process message\"}";
        }
    }

    private void updateJsonFile(Map<String, Object> entry, String operation) {
        try {
            objectMapper.writeValue(new File(jsonFile), data);

            // Publish the updated entry to the update queue
            publishUpdate(entry, operation);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void publishUpdate(Map<String, Object> entry, String operation) {
        try (Connection connection = new ConnectionFactory().newConnection();
             Channel channel = connection.createChannel()) {

            // Create the update message
            Map<String, Object> updateMessage = new HashMap<>();
            updateMessage.put("operation", operation);
            updateMessage.put("data", entry);
            updateMessage.put("sourceThreadId", threadId); // Include the source thread ID

            // Serialize the update message to JSON
            String jsonData = objectMapper.writeValueAsString(updateMessage);

            // Publish the update to the fanout exchange
            channel.basicPublish(EXCHANGE_NAME, "", null, jsonData.getBytes(StandardCharsets.UTF_8));
            System.out.println("[Thread " + threadId + "] Published update: " + jsonData);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processUpdate(String jsonData) {
        try {
            // Deserialize the received JSON data
            Map<String, Object> updateMessage = objectMapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {});
            String operation = (String) updateMessage.get("operation");
            int sourceThreadId = (int) updateMessage.get("sourceThreadId"); // Get the source thread ID
            Map<String, Object> entry = objectMapper.convertValue(updateMessage.get("data"), new TypeReference<Map<String, Object>>() {});

            // Ignore updates from the same thread
            if (sourceThreadId == threadId) {
                System.out.println("[Thread " + threadId + "] Ignored update from itself.");
                return;
            }

            if ("add".equals(operation)) {
                // Add the new entry to the local data
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> posts = (List<Map<String, Object>>) data.get("posts");
                posts.add(entry);

                // Overwrite the local JSON file
                objectMapper.writeValue(new File(jsonFile), data);
                System.out.println("[Thread " + threadId + "] Added new entry and updated local data file.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadDataFromFile() {
        try {
            File file = new File(jsonFile);
            if (file.exists()) {
                Map<String, Object> loadedData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {});
                data.putAll(loadedData);
                System.out.println("[Thread " + threadId + "] Loaded data from file: " + data);
            } else {
                // Initialize with empty lists for posts and users
                data.put("posts", new ArrayList<Map<String, Object>>());
                data.put("users", new ArrayList<Map<String, Object>>());
                System.out.println("[Thread " + threadId + "] No existing data file found. Starting with empty data.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        for (int i = 1; i <= 5; i++) {
            new Thread(new Main(i), "Thread " + i).start();
        }

        // Keep the main thread alive to allow logging from other threads
        synchronized (Main.class) {
            try {
                Main.class.wait(); // Main thread waits indefinitely
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}