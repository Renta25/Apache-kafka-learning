package com.example.Apachekafkatest

import com.example.Apachekafkatest.Producer.KafkaOrderProducer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class Application

fun main(args: Array<String>) {
	runApplication<Application>(*args)
}
