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
        // Update path to save files in /data directory
        this.jsonFile = "/data/data_thread_" + threadId + ".json";
    }

    @Override
    public void run() {
        Connection connection = null;
        Channel taskChannel = null;
        Channel updateChannel = null;

        try {
            // Load data from the thread-specific JSON file
            loadDataFromFile();

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("rabbitmq");
            factory.setUsername("guest");
            factory.setPassword("guest");

            final int maxAttempts = 10;
            boolean connected = false;

            for (int attempt = 1; attempt <= maxAttempts; attempt++) {
                try {
                    connection = factory.newConnection();
                    taskChannel = connection.createChannel();
                    updateChannel = connection.createChannel();
                    System.out.println("✅ [Thread " + threadId + "] Conectado ao RabbitMQ na tentativa " + attempt);
                    connected = true;
                    break;
                } catch (Exception e) {
                    System.out.println("⏳ [Thread " + threadId + "] Tentativa " + attempt + "/" + maxAttempts + " falhou. Aguardando...");
                    Thread.sleep(attempt * 2000L);
                }
            }

            if (!connected || connection == null || taskChannel == null || updateChannel == null) {
                System.err.println("❌ [Thread " + threadId + "] Não foi possível conectar ao RabbitMQ após várias tentativas.");
                return;
            }

            // Agora taskChannel e updateChannel são efetivamente finais aqui

            // Declare queues and exchanges
            taskChannel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
            updateChannel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            String updateQueue = updateChannel.queueDeclare().getQueue();
            updateChannel.queueBind(updateQueue, EXCHANGE_NAME, "");

            System.out.println("[Thread " + threadId + "] Server is waiting for messages...");

            // Consumers agora acessam apenas variáveis finais
            final Channel finalTaskChannel = taskChannel;
            final Channel finalUpdateChannel = updateChannel;

            DeliverCallback taskCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String correlationId = delivery.getProperties().getCorrelationId();
                String replyTo = delivery.getProperties().getReplyTo();

                System.out.println("[Thread " + threadId + "] Received task message: " + message);
                String response = processMessage(message, finalUpdateChannel);

                System.out.println("[Thread " + threadId + "] Replying to queue \"" + replyTo + "\" the message: " + response);

                AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .build();
                finalTaskChannel.basicPublish("", replyTo, replyProps, response.getBytes(StandardCharsets.UTF_8));
            };

            DeliverCallback updateCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("[Thread " + threadId + "] Received update: " + message);
                processUpdate(message);
            };

            taskChannel.basicConsume(TASK_QUEUE_NAME, true, taskCallback, consumerTag -> {});
            updateChannel.basicConsume(updateQueue, true, updateCallback, consumerTag -> {});

            synchronized (this) {
                this.wait();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (taskChannel != null) taskChannel.close();
                if (updateChannel != null) updateChannel.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                System.err.println("[Thread " + threadId + "] Erro ao fechar recursos:");
                e.printStackTrace();
            }
        }
    }

    private String processMessage(String message, Channel updateChannel) {
        try {
            Map<String, Object> requestData = objectMapper.readValue(message, new TypeReference<>() {});
            String operation = (String) requestData.get("operation");

            if ("create_post".equals(operation)) {
                Map<String, Object> post = objectMapper.convertValue(requestData.get("data"), new TypeReference<>() {});
                String postId = "post_" + (((List<?>) data.get("posts")).size() + 1);
                post.put("postId", postId);

                List<Map<String, Object>> posts = (List<Map<String, Object>>) data.get("posts");
                posts.add(post);
                data.put("posts", posts); // garantia

                updateJsonFile(post, "add", updateChannel);

                return "{\"status\": \"success\", \"postId\": \"" + postId + "\"}";

            } else if ("get_posts".equals(operation)) {
                List<Map<String, Object>> posts = (List<Map<String, Object>>) data.get("posts");
                return objectMapper.writeValueAsString(posts);

            } else if ("follow".equals(operation)) {
                Map<String, Object> followData = objectMapper.convertValue(requestData.get("data"), new TypeReference<>() {});
                String follower = (String) followData.get("follower");
                String following = (String) followData.get("following");

                Map<String, List<String>> follows = data.containsKey("follows")
                    ? objectMapper.convertValue(data.get("follows"), new TypeReference<>() {})
                    : new HashMap<>();

                List<String> followingList = follows.getOrDefault(follower, new ArrayList<>());
                if (!followingList.contains(following)) {
                    followingList.add(following);
                }
                follows.put(follower, followingList);
                data.put("follows", follows);

                Map<String, Object> castedFollows = objectMapper.convertValue(follows, new TypeReference<>() {});
                updateJsonFile(castedFollows, "add_follow", updateChannel);

                return "{\"status\": \"success\", \"message\": \"Follow registrado com sucesso\"}";

            } else if ("get_follows".equals(operation)) {
                String username = (String) ((Map<?, ?>) requestData.get("data")).get("username");

                Map<String, List<String>> follows = data.containsKey("follows")
                    ? objectMapper.convertValue(data.get("follows"), new TypeReference<>() {})
                    : new HashMap<>();

                List<String> followingList = follows.getOrDefault(username, new ArrayList<>());
                return objectMapper.writeValueAsString(followingList);

            } else if ("enviar_mensagem".equals(operation)) {
                Map<String, Object> msg = objectMapper.convertValue(requestData.get("data"), new TypeReference<>() {});

                List<Map<String, Object>> messages = data.containsKey("messages")
                    ? objectMapper.convertValue(data.get("messages"), new TypeReference<>() {})
                    : new ArrayList<>();

                messages.add(msg);
                data.put("messages", messages);

                updateJsonFile(msg, "add_message", updateChannel);

                return "{\"status\": \"success\"}";

            } else if ("get_historico".equals(operation)) {
                Map<String, Object> payload = objectMapper.convertValue(requestData.get("data"), new TypeReference<>() {});
                String sender = (String) payload.get("sender");
                String receiver = (String) payload.get("receiver");

                List<Map<String, Object>> messages = data.containsKey("messages")
                    ? objectMapper.convertValue(data.get("messages"), new TypeReference<>() {})
                    : new ArrayList<>();

                List<Map<String, Object>> historico = messages.stream()
                    .filter(m -> (sender.equals(m.get("sender")) && receiver.equals(m.get("receiver"))) ||
                                (receiver.equals(m.get("sender")) && sender.equals(m.get("receiver"))))
                    .toList();

                return objectMapper.writeValueAsString(historico);

            } else if ("get_mutual_follows".equals(operation)) {
                String username = (String) ((Map<?, ?>) requestData.get("data")).get("username");

                Map<String, List<String>> follows = data.containsKey("follows")
                    ? objectMapper.convertValue(data.get("follows"), new TypeReference<>() {})
                    : new HashMap<>();

                List<String> seguindo = follows.getOrDefault(username, new ArrayList<>());

                List<String> mutuos = seguindo.stream()
                    .filter(user -> follows.containsKey(user) && follows.get(user).contains(username))
                    .toList();

                return objectMapper.writeValueAsString(mutuos);

            } else {
                return "{\"status\": \"error\", \"message\": \"Unknown operation\"}";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "{\"status\": \"error\", \"message\": \"Failed to process message\"}";
        }
    }

    private void updateJsonFile(Map<String, Object> entry, String operation, Channel updateChannel) {
        try {
            objectMapper.writeValue(new File(jsonFile), data);
            publishUpdate(updateChannel, entry, operation);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void publishUpdate(Channel updateChannel, Map<String, Object> entry, String operation) {
        try {
            Map<String, Object> updateMessage = new HashMap<>();
            updateMessage.put("operation", operation);
            updateMessage.put("data", entry);
            updateMessage.put("sourceThreadId", threadId);

            String jsonData = objectMapper.writeValueAsString(updateMessage);

            updateChannel.basicPublish(EXCHANGE_NAME, "", null, jsonData.getBytes(StandardCharsets.UTF_8));
            System.out.println("[Thread " + threadId + "] Published update: " + jsonData);
        } catch (Exception e) {
            System.err.println("[Thread " + threadId + "] Erro ao publicar update:");
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
                if (entry.containsKey("postId")) {
                    // É um post
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> posts = (List<Map<String, Object>>) data.get("posts");
                    posts.add(entry);
                    System.out.println("[Thread " + threadId + "] Adicionou post replicado.");
                }
                objectMapper.writeValue(new File(jsonFile), data);
                System.out.println("[Thread " + threadId + "] Dados locais atualizados.");
            } else if ("add_follow".equals(operation)) {
                // Atualiza a estrutura de follows
                Map<String, List<String>> receivedFollows = objectMapper.convertValue(entry, new TypeReference<Map<String, List<String>>>() {});
                data.put("follows", receivedFollows);
                objectMapper.writeValue(new File(jsonFile), data);
                System.out.println("[Thread " + threadId + "] Estrutura de follows replicada.");
            } else if ("add_message".equals(operation)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> msg = (Map<String, Object>) entry;

                List<Map<String, Object>> messages = data.containsKey("messages")
                    ? objectMapper.convertValue(data.get("messages"), new TypeReference<>() {})
                    : new ArrayList<>();

                messages.add(msg);
                data.put("messages", messages);

                objectMapper.writeValue(new File(jsonFile), data);
                System.out.println("[Thread " + threadId + "] Mensagem replicada.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadDataFromFile() {
        try {
            // Create /data directory if it doesn't exist
            File dataDir = new File("/data");
            if (!dataDir.exists()) {
                boolean created = dataDir.mkdirs();
                if (created) {
                    System.out.println("[Thread " + threadId + "] Created /data directory");
                } else {
                    System.err.println("[Thread " + threadId + "] Failed to create /data directory");
                }
            }

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