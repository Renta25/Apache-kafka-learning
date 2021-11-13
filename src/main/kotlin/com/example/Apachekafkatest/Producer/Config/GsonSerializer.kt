package com.example.Apachekafkatest.Producer.Config

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Serializer

class GsonSerializer: Serializer<Any> {

    private val gson = Gson()

    override fun serialize(topic: String?, data: Any?): ByteArray {
        return gson.toJson(data).toByteArray()
    }


}