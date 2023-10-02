package com.example.myproject

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import scala.io.Source

object SendingKafka {
  def main(args: Array[String]): Unit = {
    // Define Kafka producer properties
    val props = new java.util.Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ip-172-31-5-217.eu-west-2.compute.internal:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // Create a Kafka producer
    val producer = new KafkaProducer[String, String](props)

    // Define the API URL
    val apiUrl = "http://localhost:5000/api/data"

    // Create an HTTP client
    val httpClient = HttpClients.createDefault()

    // Make an HTTP GET request to the API
    val httpGet = new HttpGet(apiUrl)
    val response = httpClient.execute(httpGet)

    // Read the API response
    val responseBody = Source.fromInputStream(response.getEntity.getContent).mkString
    response.close()

    // Send the API data to Kafka
    val topic = "Data_airline"
    val key = "Done" // You can define your own key logic
    val record = new ProducerRecord[String, String](topic, key, responseBody)
    producer.send(record)

    // Close the Kafka producer
    producer.close()
  }
  }

