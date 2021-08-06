/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private int numRecords;
    private final CountDownLatch latch;


    /**
     * 构造方法
     * @param topic 主题
     * @param isAsync   是否异步
     * @param transactionalId   事物
     * @param enableIdempotency
     * @param numRecords
     * @param transactionTimeoutMs
     * @param latch
     */
    public Producer(final String topic,
                    final Boolean isAsync,
                    final String transactionalId,
                    final boolean enableIdempotency,
                    final int numRecords,
                    final int transactionTimeoutMs,
                    final CountDownLatch latch) {
        // 创建Properties配置对象
        Properties props = new Properties();
        // 指定Kafka集群的地址 bootstrap.servers + url + port
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        // client.id 一般不做设置
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        // 设置序列化的类， 数据传输过程中需要进行序列化，消费者获取数据需要反序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        if (transactionTimeoutMs > 0) {
            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        }
        if (transactionalId != null) {
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotency);

        // 🔥 构建 KafkaProducer
        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        this.numRecords = numRecords;
        this.latch = latch;
    }

    KafkaProducer<Integer, String> get() {
        return producer;
    }

    @Override
    public void run() {
        int messageKey = 0;
        int recordsSent = 0;

        // 一直往Kafka发送数据
        while (recordsSent < numRecords) {
            String messageStr = "Message_" + messageKey;
            long startTime = System.currentTimeMillis(); // 当前系统时间

            /*
            同步发送还是异步发送
             */
            if (isAsync) { // 🔥Send asynchronously

                // 将消息封装成了 ProducerRecord 对象
                // 🚀 异步会发送很多条消息给Kafka，用 DemoCallBack 回调函数 判断消息是否发送成功。
                producer.send(new ProducerRecord<>(topic,
                    messageKey,
                    messageStr), new DemoCallBack(startTime, messageKey, messageStr));
            } else { // 🔥Send synchronously 效率地下
                try {
                    // 发送一条消息，等这条消息的后续工作全部完成再继续下一条数据的发送。
                    producer.send(new ProducerRecord<>(topic,
                        messageKey,
                        messageStr)).get(); // 阻塞方法
                    System.out.println("Sent message: (" + messageKey + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            messageKey += 2;
            recordsSent += 1;
        }
        System.out.println("Producer sent " + numRecords + " records successfully");
        latch.countDown();
    }
}

// 回调函数 实现了 Callback
class DemoCallBack implements Callback {

    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
     * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
     *                  with -1 value for all fields except for topicPartition will be returned if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
