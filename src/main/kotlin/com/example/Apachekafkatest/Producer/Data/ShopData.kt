package com.example.Apachekafkatest.Producer.Data

import com.example.Apachekafkatest.Producer.Models.ShopPartition

class ShopData {
    companion object{
        val shopList: List<ShopPartition> = listOf(
            ShopPartition("Poznań 1",1),
            ShopPartition("Poznań 2",1),
            ShopPartition("Gniezno 1",2),
            ShopPartition("Września 1",3),
            ShopPartition("Gorzów 1",4)
        )
    }
}