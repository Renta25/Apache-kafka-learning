package com.example.Apachekafkatest.Models

import com.example.Apachekafkatest.Data.ShopData

data class Shop(
    val name: String
)

fun Shop.getPartition(): Int{
    val list = ShopData.shopList
    val position = list.find { it.name == this.name }
    return position?.kafkaPartition ?: 0
}
