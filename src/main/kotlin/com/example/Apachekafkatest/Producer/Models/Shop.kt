package com.example.Apachekafkatest.Producer.Models

import com.example.Apachekafkatest.Producer.Data.ShopData

data class Shop(
    val name: String
)

fun Shop.getPartition(): Int{
    val list = ShopData.shopList
    val position = list.find { it.name == this.name }
    return position?.kafkaPartition ?: 0
}
