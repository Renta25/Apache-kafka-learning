package com.example.Apachekafkatest.Producer.Models

data class Order(
    val userId: Long,
    val amount: Float,
    val productId: Long
)
