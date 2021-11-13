package com.example.Apachekafkatest.Producer.Config

import com.example.Apachekafkatest.Producer.Models.Shop
import com.example.Apachekafkatest.Producer.Models.getPartition
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.slf4j.LoggerFactory

class CustomPartitioner: Partitioner {

    private val logger = LoggerFactory.getLogger(CustomPartitioner::class.java)

    override fun configure(configs: MutableMap<String, *>?) {
        logger.info("CustomPartitioner:configure")
    }

    override fun close() {
        logger.info("CustomPartitioner:close")
    }

    override fun partition(
        topic: String?,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster?
    ): Int {
        return if (key is Shop)
            key.getPartition()
        else
            return 1
    }
}