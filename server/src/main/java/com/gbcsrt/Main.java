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
import java.util.concurrent.atomic.AtomicBoolean;

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

    private volatile boolean isCoordinator = false;
    private volatile long lastCoordinatorPing = System.currentTimeMillis();
    private final long electionTimeout = 10000; // 10 segundos sem ping
    private volatile boolean running = true;
    private Thread coordinatorThread = null;
    private static final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private PrintWriter logWriter;
    private volatile boolean waitingForElectionResponse = false;
    private static final Object coordinatorLock = new Object();

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
                        synchronized (coordinatorLock) {
                            if (!isCoordinator) {
                                isCoordinator = true;
                                startCoordinator(syncChannel);
                                log("👑 Inicializado como coordenador primário");
                            }
                        }
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
                            log("⚔️ Recebeu eleição de " + source + ". Respondendo...");
                            
                            // Se sou maior, inicio minha própria eleição
                            if (threadId > source) {
                                waitingForElectionResponse = false; // Reset state
                                
                                // Se ainda não estou em uma eleição, eu inicio
                                if (electionInProgress.compareAndSet(false, true)) {
                                    initiateElection(finalElectionChannel, finalConnection, finalSyncChannel);
                                } else {
                                    log("🔄 Já existe eleição em andamento, não iniciando outra");
                                }
                            }
                        } else if (body.startsWith("NEW_COORDINATOR_")) {
                            int coord = Integer.parseInt(body.split("_")[2]);
                            log("👑 Novo coordenador é a thread " + coord);
                            
                            synchronized (coordinatorLock) {
                                waitingForElectionResponse = false;
                                
                                // Se eu for o novo coordenador declarado
                                if (coord == threadId && !isCoordinator) {
                                    isCoordinator = true;
                                    startCoordinator(finalSyncChannel);
                                } else if (isCoordinator && coord != threadId) {
                                    // Se eu era o coordenador mas agora é outro
                                    stopCoordinator();
                                    isCoordinator = false;
                                } else if (coord != threadId) {
                                    // Se sou apenas um participante normal
                                    isCoordinator = false;
                                }
                                
                                // Reset do estado de eleição
                                electionInProgress.set(false);
                                lastCoordinatorPing = System.currentTimeMillis();
                            }
                        }
                    }, consumerTag -> {});

                    log("✅ Conectado ao RabbitMQ na tentativa " + attempt);
                    connected = true;
                    break;
                } catch (Exception e) {
                    log("⏳ Tentativa " + attempt + "/" + maxAttempts + " falhou. Aguardando...");
                    Thread.sleep(attempt * 2000L);
                }
            }

            if (!connected || connection == null || taskChannel == null || updateChannel == null) {
                log("❌ Não foi possível conectar ao RabbitMQ após várias tentativas.");
                return;
            }

            startElectionMonitor(electionChannel, connection, syncChannel);

            // Tarefas normais
            taskChannel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
            updateChannel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            String updateQueue = updateChannel.queueDeclare().getQueue();
            updateChannel.queueBind(updateQueue, EXCHANGE_NAME, "");

            log("Server is waiting for messages...");

            final Channel finalTaskChannel = taskChannel;
            final Channel finalUpdateChannel = updateChannel;

            DeliverCallback taskCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String correlationId = delivery.getProperties().getCorrelationId();
                String replyTo = delivery.getProperties().getReplyTo();

                log("Received task message: " + message);
                String response = processMessage(message, finalUpdateChannel);

                AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .build();
                finalTaskChannel.basicPublish("", replyTo, replyProps, response.getBytes(StandardCharsets.UTF_8));
            };

            DeliverCallback updateCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
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
                    log("💣 Coordenador encerrado por mensagem no fanout.");
                    running = false;
                    stopCoordinator();
                    return "{\"status\": \"ok\", \"message\": \"Coordenador encerrado via fanout\"}";
                } else {
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
                log("Ignored update from itself.");
                return;
            }

            if ("add".equals(operation)) {
                if (entry.containsKey("postId")) {
                    // É um post
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> posts = (List<Map<String, Object>>) data.get("posts");
                    posts.add(entry);
                    log("Adicionou post replicado.");
                }
                objectMapper.writeValue(new File(jsonFile), data);
                log("Dados locais atualizados.");
            } else if ("add_follow".equals(operation)) {
                // Atualiza a estrutura de follows
                Map<String, List<String>> receivedFollows = objectMapper.convertValue(entry, new TypeReference<Map<String, List<String>>>() {});
                data.put("follows", receivedFollows);
                objectMapper.writeValue(new File(jsonFile), data);
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
                    log("Created /data directory");
                } else {
                    log("Failed to create /data directory");
                }
            }

            File file = new File(jsonFile);
            if (file.exists()) {
                Map<String, Object> loadedData = objectMapper.readValue(file, new TypeReference<Map<String, Object>>() {});
                data.putAll(loadedData);
                log("Loaded data from file: " + data);
            } else {
                // Initialize with empty lists for posts and users
                data.put("posts", new ArrayList<Map<String, Object>>());
                data.put("users", new ArrayList<Map<String, Object>>());
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
                        log("⏳ Relógio atrasado em " + offset + "s → " + logicalClock);
                    } else {
                        logicalClock += 1;
                    }
                } catch (InterruptedException e) {
                    break; // Encerra a thread quando interrompida
                }
            }
        }, "ClockUpdater-" + threadId).start();
    }

    private void checkClockAndUpdate(long incomingTimestamp, boolean forcar) {
        if (forcar || incomingTimestamp > logicalClock) {
            if (forcar) {
                log("⏰ Clock atualizado via 👑 Coordenador: de " + logicalClock + " para " + incomingTimestamp);
            } else {
                log("⏰ Clock atualizado via mensagem: de " + logicalClock + " para " + incomingTimestamp);
            }
            logicalClock = incomingTimestamp;
        }
    }

    private void startCoordinator(Channel syncChannel) {
        synchronized (coordinatorLock) {
            if (coordinatorThread != null && coordinatorThread.isAlive()) {
                log("🚫 Thread coordenadora já está em execução. Evitando duplicação.");
                return;
            }
            
            log("🚀 Iniciando nova thread coordenadora");
            
            coordinatorThread = new Thread(() -> {
                while (running && !Thread.currentThread().isInterrupted() && isCoordinator) {
                    try {
                        Thread.sleep(5000);
                        if (isCoordinator) {  // Verificar novamente pois pode ter mudado durante o sleep
                            syncChannel.basicPublish(SYNC_EXCHANGE, "", null, String.valueOf(logicalClock).getBytes());
                            log("🔄 Enviando clock (" + logicalClock + ") para sincronizar os outros.");
                        } else {
                            log("🛑 Não sou mais coordenador. Parando ciclo.");
                            break;
                        }
                    } catch (InterruptedException e) {
                        log("⛔ Coordenador interrompido.");
                        break;
                    } catch (Exception e) {
                        log("❌ Erro ao enviar clock: " + e.getMessage());
                        break;
                    }
                }
                log("👋 Thread coordenadora encerrada");
            }, "Coordinator-" + threadId);

            coordinatorThread.start();
        }
    }

    private void stopCoordinator() {
        synchronized (coordinatorLock) {
            if (coordinatorThread != null && coordinatorThread.isAlive()) {
                coordinatorThread.interrupt();
                log("🛑 Thread coordenadora interrompida");
                try {
                    // Aguarda até 2 segundos para a thread terminar
                    coordinatorThread.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                coordinatorThread = null;
            }
        }
    }

    private void startElectionMonitor(Channel electionChannel, Connection connection, Channel syncChannel) {
        new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(3000);
                    
                    if (isCoordinator) {
                        // Se sou coordenador, não preciso monitorar
                        continue;
                    }
                    
                    long elapsed = System.currentTimeMillis() - lastCoordinatorPing;

                    if (elapsed > electionTimeout && !electionInProgress.get() && !waitingForElectionResponse) {
                        log("⚠️ Coordenador inativo por " + (elapsed/1000) + "s. Iniciando eleição.");
                        
                        // Tentar marcar como em progresso, se conseguir, inicio
                        if (electionInProgress.compareAndSet(false, true)) {
                            waitingForElectionResponse = true;
                            initiateElection(electionChannel, connection, syncChannel);
                        } else {
                            log("🔄 Outra thread já iniciou eleição. Aguardando.");
                        }
                    }

                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    log("❌ Erro no monitor de eleição: " + e.getMessage());
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
                    log("▶️ Mandou eleição para thread " + i);
                    higherThreads.add(i);
                } catch (Exception e) {
                    log("⚠️ Falha ao contatar thread " + i + " (pode estar offline)");
                }
            }

            if (higherThreads.isEmpty()) {
                log("🏆 Nenhuma thread superior ao ID " + threadId + ". Assumindo como coordenador imediatamente.");
                announceNewCoordinator(connection, syncChannel);
                return;
            }

            // Espera até 5s por resposta
            new Thread(() -> {
                try {
                    Thread.sleep(5000);
                    
                    // Se ainda estou esperando resposta após 5s
                    if (waitingForElectionResponse && electionInProgress.get()) {
                        log("⏰ Timeout da eleição. Nenhum superior respondeu. Assumindo coordenação.");
                        announceNewCoordinator(connection, syncChannel);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "ElectionTimeout-" + threadId).start();

        } catch (Exception e) {
            log("❌ Erro ao iniciar eleição: " + e.getMessage());
            electionInProgress.set(false);
            waitingForElectionResponse = false;
        }
    }

    private void announceNewCoordinator(Connection connection, Channel syncChannel) {
        try (Channel channel = connection.createChannel()) {
            String msg = "NEW_COORDINATOR_" + threadId;

            for (int i = 1; i <= 5; i++) {
                String queue = ELECTION_QUEUE_PREFIX + i;
                try {
                    channel.basicPublish("", queue, null, msg.getBytes());
                    log("📢 Notificando thread " + i + " sobre nova coordenação");
                } catch (Exception e) {
                    log("⚠️ Falha ao notificar thread " + i);
                }
            }

            synchronized (coordinatorLock) {
                isCoordinator = true;
                waitingForElectionResponse = false;
                electionInProgress.set(false);
                lastCoordinatorPing = System.currentTimeMillis();
                
                log("👑 Agora sou o coordenador.");
                startCoordinator(syncChannel);
            }

        } catch (Exception e) {
            log("❌ Erro ao anunciar novo coordenador: " + e.getMessage());
            electionInProgress.set(false);
            waitingForElectionResponse = false;
        }
    }

    private void log(String message) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String fullMessage = "[" + timestamp + "] [Thread " + threadId + "] " + message;

        // Para reduzir poluição do terminal, evitar logs de clock a cada 1s
        boolean isClockUpdate = message.contains("Relógio atrasado") || 
                               (!isCoordinator && message.contains("Clock atualizado"));
        
        if (!isClockUpdate) {
            System.out.println(fullMessage);
        }
        
        if (logWriter != null) {
            logWriter.println(fullMessage);
            logWriter.flush();
        }
    }
}