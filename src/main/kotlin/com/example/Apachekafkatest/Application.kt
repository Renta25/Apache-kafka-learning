package com.example.Apachekafkatest

import com.example.Apachekafkatest.Models.AppType
import com.example.Apachekafkatest.Producer.KafkaOrderProducer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class Application

fun main(args: Array<String>) {
	if (args.count() > 0 && args[0] == "p"){
		Config.appType = AppType.PRODUCER
	}else{
		Config.appType = AppType.CONSUMER
	}

	runApplication<Application>(*args)
}
