package etu.supmti.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WordCountConsumer {

    private static final String TOPIC_NAME = "WordCount-Topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9093,localhost:9094";
    private static final String GROUP_ID = "word-count-java-group";

    private final Map<String, Integer> wordCounts = new HashMap<>();

    public static void main(String[] args) {
        new WordCountConsumer().run();
    }

    public void run() {

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("auto.offset.reset", "earliest"); 

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            
            System.out.println("--- Démarrage du Consommateur de Mots ---");
            System.out.println("Topic: " + TOPIC_NAME + ", Group ID: " + GROUP_ID);

            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n*** Arrêt du consommateur demandé ***");

                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                records.forEach(record -> {
                    String word = record.value();
                    
                    wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
                    
                    System.out.printf("Consumed: '%s' from Partition %d, Offset %d | Frequency: %d%n",
                                      word, 
                                      record.partition(), 
                                      record.offset(), 
                                      wordCounts.get(word));
                });
  
                consumer.commitSync(); 
            }

        } catch (WakeupException e) {

            System.out.println("*** Arrêt propre du Consommateur réussi. ***");
        } catch (Exception e) {
            System.err.println("Une erreur inattendue s'est produite : " + e.getMessage());
            e.printStackTrace();
        }
    }
}