package com.example.Apachekafkatest.Schedulers

import com.example.Apachekafkatest.Producer.KafkaOrderProducer
import com.example.Apachekafkatest.Producer.Models.Order
import com.example.Apachekafkatest.Producer.Models.Shop
import org.apache.kafka.clients.producer.Callback
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.logging.Logger
import kotlin.random.Random

@Component
class OrderSenderSchedulers {

    private val orderSendController: KafkaOrderProducer = KafkaOrderProducer()
    private val logger = LoggerFactory.getLogger(OrderSenderSchedulers::class.java)

    @Scheduled(cron = "0/2 * * * * * ")
    fun start(){
        orderSendController.sendRecord(getOrderSource(), getOrder()) { metadata, exception ->
            logger.info(metadata.toString())
        }
    }

    private fun getOrder():Order{
        val userId = Random.nextLong(1, Long.MAX_VALUE)
        val amount = Random.nextFloat()
        val productId = Random.nextLong(1, Long.MAX_VALUE)
        return Order(
            userId,
            amount,
            productId
        )
    }

    private val shopList: List<String> = listOf(
        "Poznań 1",
        "Poznań 2",
        "Gniezno 1",
        "Września 1",
        "Gorzów 1"
    )
    private fun getOrderSource(): Shop {
        val index = Random.nextInt(0, shopList.count() - 1)
        return Shop(shopList[index])
    }
}