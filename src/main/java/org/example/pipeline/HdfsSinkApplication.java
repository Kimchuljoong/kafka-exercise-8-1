package org.example.pipeline;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.jetty.util.thread.ShutdownThread;
import org.example.pipeline.consumer.ConsumerWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HdfsSinkApplication {

    private final static Logger logger = LoggerFactory.getLogger(HdfsSinkApplication.class);

    private final static String BOOTSTRAP_SERVERS = "ec2-3-38-209-151.ap-northeast-2.compute.amazonaws.com:9092";
    private final static String TOPIC_NAME = "select-color";
    private final static String GROUP_ID = "color-hdfs-save-consumer-group";
    private final static int CONSUMER_COUNT = 3;
    private final static List<ConsumerWorker> workers = new ArrayList<>();

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ExecutorService executorService = Executors.newCachedThreadPool();

        for (int i = 0; i < CONSUMER_COUNT; i++) {
            workers.add(new ConsumerWorker(configs, TOPIC_NAME, i));
        }
        workers.forEach(executorService::execute);

    }

    static class ShutdownThread extends Thread {
        public void run() {
            logger.info("Shutdown hool");
            workers.forEach(ConsumerWorker::stopAndWakeup);
        }
    }


}
