package com.example.Apachekafkatest.Schedulers

import com.example.Apachekafkatest.Producer.Data.ShopData
import com.example.Apachekafkatest.Producer.KafkaOrderProducer
import com.example.Apachekafkatest.Producer.Models.Order
import com.example.Apachekafkatest.Producer.Models.Shop
import com.example.Apachekafkatest.Producer.Models.getShop
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
            exception?.printStackTrace()

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

    private fun getOrderSource(): Shop {
        val list = ShopData.shopList
        val index = Random.nextInt(0, list.count())
        return list[index].getShop()
    }
}