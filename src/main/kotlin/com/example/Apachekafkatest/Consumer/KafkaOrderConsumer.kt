package com.example.Apachekafkatest.Consumer

import com.example.Apachekafkatest.Config
import com.example.Apachekafkatest.Models.AppType
import com.example.Apachekafkatest.Models.Order
import com.example.Apachekafkatest.Models.Shop
import org.apache.kafka.clients.consumer.*
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

        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Int.MAX_VALUE) // defult: Int.MAX_VALUE
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 250) //defult: 500
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1) //defult: 1byte
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 52428800) //defult: 52428800 byte
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500) //defult: 500ms
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1048576) //defult: 1048576 byte

//        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor::class.java) //zachowanie na więcej niż jedne słuchacz w grupie

        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000) //defult: 30000ms
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000)  //defult: 10000ms
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000) // I'am live

        return properties
    }

    @OptIn(ExperimentalStdlibApi::class)
    private fun orderListening(){
        consumer?.subscribe(Collections.singletonList(topicTitle))
        //consumer?.assign()
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