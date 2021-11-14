package com.example.Apachekafkatest.Producer

import com.example.Apachekafkatest.Config
import com.example.Apachekafkatest.Models.AppType
import com.example.Apachekafkatest.Producer.Config.CustomPartitioner
import com.example.Apachekafkatest.Producer.Config.GsonSerializer
import com.example.Apachekafkatest.Models.Order
import com.example.Apachekafkatest.Models.Shop
import org.apache.kafka.clients.producer.*
import java.util.*

class KafkaOrderProducer {

    private val producer: Producer<Shop, Order>?
    private val topicTitle: String = "Orders"

    init {
        producer = if (Config.appType == AppType.PRODUCER)
            KafkaProducer(getProducerProperties())
        else
            null
    }

    private fun getProducerProperties(): Properties{
        val properties = Properties()
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.151:9092")
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, GsonSerializer::class.java)
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer::class.java)
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer-1")
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner::class.java)
        properties.put(ProducerConfig.ACKS_CONFIG, "1") //0 no wait //1 wait for the leader // -1 wait for all in sync

        //Batch
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0)
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5)
        properties.put(ProducerConfig.RETRIES_CONFIG, 2)

        return properties
    }

    fun sendRecord(key: Shop, order: Order, callback: Callback){
        val record = ProducerRecord(topicTitle, key, order)
        producer?.send(record, callback)
    }
}