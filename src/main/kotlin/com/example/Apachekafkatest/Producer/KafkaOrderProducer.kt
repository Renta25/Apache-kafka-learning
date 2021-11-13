package com.example.Apachekafkatest.Producer

import com.example.Apachekafkatest.Producer.Models.Order
import com.example.Apachekafkatest.Producer.Models.Shop
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class KafkaOrderProducer {

    private val producer: KafkaProducer<Shop, Order>
    private val topicTitle: String = "Orders"

    init {
        producer = KafkaProducer(getProducerProperties())
    }

    private fun getProducerProperties(): Properties{
        val properties = Properties()
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.151:9092")
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, GsonSerializer::class.java)
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer::class.java)
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer-1")

        return properties
    }

    fun sendRecord(key: Shop, order: Order, callback: Callback){
        val record = ProducerRecord(topicTitle, key, order)
        producer.send(record, callback)
    }
}