package com.example.Apachekafkatest.Producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class KafkaOrderProducer {

    private val producer: KafkaProducer<String, String>
    private val topicTitle: String = "Orders"

    init {
        producer = KafkaProducer(getProducerProperties())
    }

    private fun getProducerProperties(): Properties{
        val properties = Properties()
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.151:9092")
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer-1")

        return properties
    }

    fun sendRecord(key: String, message: String){
        val record = ProducerRecord(topicTitle, key, message)
        producer.send(record).get()
    }
}