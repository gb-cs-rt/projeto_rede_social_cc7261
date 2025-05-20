package com.gbcsrt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
    private long logicalClock;
    private static final String SYNC_EXCHANGE = "clock_sync_exchange";
    private static final String ELECTION_QUEUE_PREFIX = "election_queue_";

    private boolean isCoordinator = false;
    private long lastCoordinatorPing = System.currentTimeMillis();
    private final long electionTimeout = 10000; // 10 segundos sem ping
    private volatile boolean running = true;
    private Thread coordinatorThread = null;
    private static volatile boolean electionInProgress = false;
    private PrintWriter logWriter;

    public Main(int threadId) {
        this.threadId = threadId;
        // Update path to save files in /data directory
        this.jsonFile = "/data/data_thread_" + threadId + ".json";
    }

    @Override
    public void run() {
        this.logicalClock = System.currentTimeMillis() / 1000L; // segundos desde época
        startClockUpdater();

        try {
            File logFile = new File("/data/log_thread_" + threadId + ".txt");
            logWriter = new PrintWriter(logFile);
            log("📝 Log iniciado para Thread " + threadId);
        } catch (IOException e) {
            System.err.println("❌ [Thread " + threadId + "] Erro ao criar arquivo de log:");
            e.printStackTrace();
        }

        Connection connection = null;
        Channel taskChannel = null;
        Channel updateChannel = null;
        Channel syncChannel = null;
        Channel electionChannel = null;

        try {
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
                    syncChannel = connection.createChannel();
                    electionChannel = connection.createChannel();

                    // Sync exchange
                    syncChannel.exchangeDeclare(SYNC_EXCHANGE, BuiltinExchangeType.FANOUT);
                    String syncQueue = syncChannel.queueDeclare().getQueue();
                    syncChannel.queueBind(syncQueue, SYNC_EXCHANGE, "");

                    // Election queue
                    String electionQueueName = ELECTION_QUEUE_PREFIX + threadId;
                    electionChannel.queueDeclare(electionQueueName, false, false, false, null);

                    // Coordenador inicial
                    if (threadId == 1) {
                        isCoordinator = true;
                        startCoordinator(syncChannel);
                    }

                    // Escutar clock do coordenador
                    syncChannel.basicConsume(syncQueue, true, (consumerTag, delivery) -> {
                        if (!isCoordinator) {
                            String clockStr = new String(delivery.getBody(), StandardCharsets.UTF_8);
                            long receivedClock = Long.parseLong(clockStr);
                            checkClockAndUpdate(receivedClock, true);
                            lastCoordinatorPing = System.currentTimeMillis();
                        }
                    }, consumerTag -> {});

                    final Channel finalElectionChannel = electionChannel;
                    final Connection finalConnection = connection;
                    final Channel finalSyncChannel = syncChannel;

                    // Escutar mensagens de eleição
                    electionChannel.basicConsume(electionQueueName, true, (consumerTag, delivery) -> {
                        String body = new String(delivery.getBody(), StandardCharsets.UTF_8);

                        if (body.startsWith("ELECTION_FROM_")) {
                            int source = Integer.parseInt(body.split("_")[2]);
                            System.out.println("[Thread " + threadId + "] ⚔️ Recebeu eleição de " + source + ". Respondendo...");
                            log("⚔️ Recebeu eleição de " + source + ". Respondendo...");
                            if (threadId > source) {
                                initiateElection(finalElectionChannel, finalConnection, finalSyncChannel);
                            }
                        } else if (body.startsWith("NEW_COORDINATOR_")) {
                            int coord = Integer.parseInt(body.split("_")[2]);
                            System.out.println("[Thread " + threadId + "] 👑 Novo coordenador é a thread " + coord);
                            log("👑 Novo coordenador é a thread " + coord);
                            isCoordinator = false;
                            lastCoordinatorPing = System.currentTimeMillis();
                        }
                    }, consumerTag -> {});

                    System.out.println("✅ [Thread " + threadId + "] Conectado ao RabbitMQ na tentativa " + attempt);
                    log("✅ Conectado ao RabbitMQ na tentativa " + attempt);
                    connected = true;
                    break;
                } catch (Exception e) {
                    System.out.println("⏳ [Thread " + threadId + "] Tentativa " + attempt + "/" + maxAttempts + " falhou. Aguardando...");
                    log("⏳ Tentativa " + attempt + "/" + maxAttempts + " falhou. Aguardando...");
                    Thread.sleep(attempt * 2000L);
                }
            }

            if (!connected || connection == null || taskChannel == null || updateChannel == null) {
                System.err.println("❌ [Thread " + threadId + "] Não foi possível conectar ao RabbitMQ após várias tentativas.");
                log("❌ Não foi possível conectar ao RabbitMQ após várias tentativas.");
                return;
            }

            startElectionMonitor(electionChannel, connection, syncChannel);

            // Tarefas normais
            taskChannel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
            updateChannel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            String updateQueue = updateChannel.queueDeclare().getQueue();
            updateChannel.queueBind(updateQueue, EXCHANGE_NAME, "");

            System.out.println("[Thread " + threadId + "] Server is waiting for messages...");
            log("Server is waiting for messages...");

            final Channel finalTaskChannel = taskChannel;
            final Channel finalUpdateChannel = updateChannel;

            DeliverCallback taskCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String correlationId = delivery.getProperties().getCorrelationId();
                String replyTo = delivery.getProperties().getReplyTo();

                System.out.println("[Thread " + threadId + "] Received task message: " + message);
                log("Received task message: " + message);
                String response = processMessage(message, finalUpdateChannel);

                AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .build();
                finalTaskChannel.basicPublish("", replyTo, replyProps, response.getBytes(StandardCharsets.UTF_8));
            };

            DeliverCallback updateCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("[Thread " + threadId + "] Received update: " + message);
                log("Received update: " + message);
                processUpdate(message);
            };

            taskChannel.basicConsume(TASK_QUEUE_NAME, true, taskCallback, consumerTag -> {});
            updateChannel.basicConsume(updateQueue, true, updateCallback, consumerTag -> {});

            while (running) {
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (taskChannel != null) taskChannel.close();
                if (updateChannel != null) updateChannel.close();
                if (syncChannel != null) syncChannel.close();
                if (electionChannel != null) electionChannel.close();
                if (connection != null) connection.close();
                if (logWriter != null) logWriter.close();
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

                String timestampStr = post.get("timestamp").toString();
                long incomingTs = LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        .atZone(ZoneId.systemDefault())
                        .toEpochSecond();
                checkClockAndUpdate(incomingTs, false);

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

                String timestampStr = followData.get("timestamp").toString();
                long incomingTs = LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        .atZone(ZoneId.systemDefault())
                        .toEpochSecond();
                checkClockAndUpdate(incomingTs, false);

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

                String timestampStr = msg.get("timestamp").toString();
                long incomingTs = LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                        .atZone(ZoneId.systemDefault())
                        .toEpochSecond();
                checkClockAndUpdate(incomingTs, false);

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

            } else if ("shutdown_coordinator".equals(operation)) {
                if (isCoordinator) {
                    System.out.println("[Thread " + threadId + "] 💣 Coordenador encerrado por mensagem no fanout.");
                    log("💣 Coordenador encerrado por mensagem no fanout.");
                    running = false;

                    if (coordinatorThread != null) {
                        coordinatorThread.interrupt();
                    }

                    return "{\"status\": \"ok\", \"message\": \"Coordenador encerrado via fanout\"}";
                } else {
                    System.out.println("[Thread " + threadId + "] Ignorando shutdown (não sou coordenador).");
                    log("Ignorando shutdown (não sou coordenador).");
                    return "{\"status\": \"ok\", \"message\": \"Shutdown ignorado (não sou coordenador)\"}";
                }
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
            log("Published update: " + jsonData);
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
                log("Ignored update from itself.");
                return;
            }

            if ("add".equals(operation)) {
                if (entry.containsKey("postId")) {
                    // É um post
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> posts = (List<Map<String, Object>>) data.get("posts");
                    posts.add(entry);
                    System.out.println("[Thread " + threadId + "] Adicionou post replicado.");
                    log("Adicionou post replicado.");
                }
                objectMapper.writeValue(new File(jsonFile), data);
                System.out.println("[Thread " + threadId + "] Dados locais atualizados.");
                log("Dados locais atualizados.");
            } else if ("add_follow".equals(operation)) {
                // Atualiza a estrutura de follows
                Map<String, List<String>> receivedFollows = objectMapper.convertValue(entry, new TypeReference<Map<String, List<String>>>() {});
                data.put("follows", receivedFollows);
                objectMapper.writeValue(new File(jsonFile), data);
                System.out.println("[Thread " + threadId + "] Estrutura de follows replicada.");
                log("Estrutura de follows replicada.");
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
                log("Mensagem replicada.");
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
                    log("Created /data directory");
                } else {
                    System.err.println("[Thread " + threadId + "] Failed to create /data directory");
                    log("Failed to create /data directory");
                }
            }

            File file = new File(jsonFile);
            if (file.exists()) {
                Map<String, Object> loadedData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {});
                data.putAll(loadedData);
                System.out.println("[Thread " + threadId + "] Loaded data from file: " + data);
                log("Loaded data from file: " + data);
            } else {
                // Initialize with empty lists for posts and users
                data.put("posts", new ArrayList<Map<String, Object>>());
                data.put("users", new ArrayList<Map<String, Object>>());
                System.out.println("[Thread " + threadId + "] No existing data file found. Starting with empty data.");
                log("No existing data file found. Starting with empty data.");
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

    private void startClockUpdater() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);

                    // 5% chance de desvio
                    double chance = Math.random();
                    if (chance < 0.10) {
                        int offset = -1;
                        logicalClock += offset;
                        System.out.println("[Thread " + threadId + "] ⏳ Relógio atrasado em " + offset + "s → " + logicalClock);
                        log("⏳ Relógio atrasado em " + offset + "s → " + logicalClock);
                    } else {
                        logicalClock += 1;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "ClockUpdater-" + threadId).start();
    }

    private void checkClockAndUpdate(long incomingTimestamp, boolean forcar) {
        if (forcar || incomingTimestamp > logicalClock) {
            if (forcar) {
                System.out.println("[Thread " + threadId + "] ⏰ Clock atualizado via 👑 Coordenador: de " + logicalClock + " para " + incomingTimestamp);
                log("⏰ Clock atualizado via 👑 Coordenador: de " + logicalClock + " para " + incomingTimestamp);
            } else {
                System.out.println("[Thread " + threadId + "] ⏰ Clock atualizado via mensagem: de " + logicalClock + " para " + incomingTimestamp);
                log("⏰ Clock atualizado via mensagem: de " + logicalClock + " para " + incomingTimestamp);
            }
            logicalClock = incomingTimestamp;
        }
    }

    private void startCoordinator(Channel syncChannel) {
        coordinatorThread = new Thread(() -> {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(5000);
                    syncChannel.basicPublish(SYNC_EXCHANGE, "", null, String.valueOf(logicalClock).getBytes());
                    System.out.println("[Thread " + threadId + "] 🔄 Enviando clock (" + logicalClock + ") para sincronizar os outros.");
                    log("🔄 Enviando clock (" + logicalClock + ") para sincronizar os outros.");
                } catch (InterruptedException e) {
                    System.out.println("[Thread " + threadId + "] ⛔ Coordenador interrompido.");
                    log("⛔ Coordenador interrompido.");
                    break;
                } catch (Exception e) {
                    System.err.println("[Thread " + threadId + "] ❌ Erro ao enviar clock:");
                    log("❌ Erro ao enviar clock:");
                    e.printStackTrace();
                }
            }
        }, "Coordinator-" + threadId);

        coordinatorThread.start();
    }

    private void startElectionMonitor(Channel electionChannel, Connection connection, Channel syncChannel) {
        new Thread(() -> {
            while (running && !isCoordinator) {
                try {
                    Thread.sleep(3000);
                    long elapsed = System.currentTimeMillis() - lastCoordinatorPing;

                    if (elapsed > electionTimeout && !electionInProgress) {
                        System.out.println("[Thread " + threadId + "] ⚠️ Coordenador inativo. Iniciando eleição.");
                        log("⚠️ Coordenador inativo. Iniciando eleição.");
                        electionInProgress = true;
                        initiateElection(electionChannel, connection, syncChannel);
                        break;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, "ElectionMonitor-" + threadId).start();
    }

    private void initiateElection(Channel channel, Connection connection, Channel syncChannel) {
        try {
            List<Integer> higherThreads = new ArrayList<>();
            for (int i = threadId + 1; i <= 5; i++) {
                String queue = ELECTION_QUEUE_PREFIX + i;
                try {
                    String msg = "ELECTION_FROM_" + threadId;
                    channel.basicPublish("", queue, null, msg.getBytes());
                    System.out.println("[Thread " + threadId + "] ▶️ Mandou eleição para thread " + i);
                    log("▶️ Mandou eleição para thread " + i);
                    higherThreads.add(i);
                } catch (Exception e) {
                    System.out.println("[Thread " + threadId + "] ⚠️ Falha ao contatar thread " + i + " (pode estar offline)");
                    log("⚠️ Falha ao contatar thread " + i + " (pode estar offline)");
                }
            }

            // Espera até 5s por resposta
            Thread.sleep(5000);

            if (higherThreads.isEmpty() && electionInProgress) {
                System.out.println("[Thread " + threadId + "] 🚨 Nenhum thread acima respondeu. Assumindo como coordenador.");
                log("🚨 Nenhum thread acima respondeu. Assumindo como coordenador.");
                announceNewCoordinator(connection, syncChannel);
            } else {
                System.out.println("[Thread " + threadId + "] ⏳ Esperando resposta dos superiores...");
                log("⏳ Esperando resposta dos superiores...");
                // OBS: Se nenhuma thread superior assumir, você ainda pode fazer um timeout e tentar de novo
                // ou deixar quieto, pois o superior que respondeu fará isso
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void announceNewCoordinator(Connection connection, Channel syncChannel) {
        try (Channel channel = connection.createChannel()) {
            String msg = "NEW_COORDINATOR_" + threadId;

            for (int i = 1; i <= 5; i++) {
                String queue = ELECTION_QUEUE_PREFIX + i;
                try {
                    channel.basicPublish("", queue, null, msg.getBytes());
                } catch (Exception e) {
                    System.out.println("[Thread " + threadId + "] ⚠️ Falha ao notificar thread " + i);
                    log("⚠️ Falha ao notificar thread " + i);
                }
            }

            isCoordinator = true;
            electionInProgress = false;
            lastCoordinatorPing = System.currentTimeMillis(); // previne nova eleição logo após assumir

            System.out.println("[Thread " + threadId + "] 👑 Agora sou o coordenador.");
            log("👑 Agora sou o coordenador.");
            startCoordinator(syncChannel); // começa a emitir sincronizações

        } catch (Exception e) {
            System.err.println("[Thread " + threadId + "] Erro ao anunciar novo coordenador:");
            e.printStackTrace();
        }
    }

    private void log(String message) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String fullMessage = "[" + timestamp + "] [Thread " + threadId + "] " + message;

        System.out.println(fullMessage); // opcional: ainda mostra no terminal
        if (logWriter != null) {
            logWriter.println(fullMessage);
            logWriter.flush();
        }
    }
}