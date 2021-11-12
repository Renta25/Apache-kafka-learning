package com.example.Apachekafkatest.Schedulers

import com.example.Apachekafkatest.Producer.KafkaOrderProducer
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.logging.Logger
import kotlin.random.Random

@Component
class OrderSenderSchedulers {

    private val orderSendController: KafkaOrderProducer = KafkaOrderProducer()
    private var iterator: Long = 0
    private val logger = LoggerFactory.getLogger(OrderSenderSchedulers::class.java)

    @Scheduled(cron = "0/2 * * * * * ")
    fun start(){
        iterator++;
        val key = "Key: " + getKey()
        val msg = "Msg: " + iterator.toString()
        logger.info("Send to kafka: { key: $key, msg: $msg}")
        orderSendController.sendRecord(key, msg)
    }

    private fun getKey():String{
        return Random.nextInt(1,10).toString()
    }
}