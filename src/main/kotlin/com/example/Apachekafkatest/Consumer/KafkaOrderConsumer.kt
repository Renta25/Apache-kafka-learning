package com.example.Apachekafkatest.Consumer

import com.example.Apachekafkatest.Config
import com.example.Apachekafkatest.Models.AppType
import com.example.Apachekafkatest.Models.Order
import com.example.Apachekafkatest.Models.Shop
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.*

@Service
class KafkaOrderConsumer {
    private val consumer: Consumer<Shop,Order>?
    private val topicTitle: String = "Orders"

    private val logger = LoggerFactory.getLogger(KafkaOrderConsumer::class.java)

    init {
        if (Config.appType == AppType.CONSUMER){
            consumer = KafkaConsumer(getConsumerProperties())
            orderListening()
        }else{
            consumer = null
        }

    }

    private fun getConsumerProperties(): Properties {
        val properties = Properties()
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.151:9092")
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-group")
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

        return properties
    }

    @OptIn(ExperimentalStdlibApi::class)
    private fun orderListening(){
        consumer?.subscribe(Collections.singletonList(topicTitle))
        while (true) {
            consumer?.poll(Duration.ofMillis(1000))?.forEach {
                logger.info(it.toString())
//                val map: Map<TopicPartition, OffsetAndMetadata> = buildMap {
//                    put(TopicPartition(topicTitle, it.partition()), OffsetAndMetadata(it.offset()))
//                }
//                consumer.commitSync(map)
            }
            consumer?.commitSync()
        }
    }
}