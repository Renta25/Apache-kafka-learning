package com.example.Apachekafkatest.Models

data class ShopPartition(
    val name: String,
    val kafkaPartition: Int
)

fun ShopPartition.getShop():Shop = Shop(this.name)
