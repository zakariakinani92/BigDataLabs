package etu.supmti.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;

public class WordProducer {

    private static final String TOPIC_NAME = "WordCount-Topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9093,localhost:9094";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {

            System.out.println("--- Démarrage du Producteur de Mots ---");
            System.out.println("Tapez du texte. Chaque mot sera envoyé au topic: " + TOPIC_NAME);
            System.out.println("Tapez 'exit' ou Ctrl+D pour quitter.");

            String line;

            while ((line = reader.readLine()) != null) {
                if (line.equalsIgnoreCase("exit")) {
                    break;
                }
               
                String[] words = line.toLowerCase().split("\\W+");

                for (String word : words) {
                    if (word.length() > 0) {

                        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, null, word);

                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.println("Erreur lors de l'envoi du mot '" + word + "': " + exception.getMessage());
                            } else {

                                System.out.println("SENT: " + word + 
                                                   " -> Partition " + metadata.partition());
                            }
                        });
                    }
                }
              
                producer.flush(); 
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
