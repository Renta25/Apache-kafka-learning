package com.example.Apachekafkatest.Models

data class Order(
    val userId: Long,
    val amount: Float,
    val productId: Long
)
