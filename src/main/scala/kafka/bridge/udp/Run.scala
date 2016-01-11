/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.bridge.udp

import scala.collection.JavaConverters._

import akka.actor.{ActorSystem, Actor, Props, ActorRef}
import akka.io.{IO, Udp}

// library for setting up to time message was received
import java.text.SimpleDateFormat     
import java.util.Calendar

// library for setting up to receive UDP message to KAFKA server
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

object Run extends App {
  override def main(args: Array[String]): Unit = {
    val system = ActorSystem("udp-kafka-bridge")

    val kafkaProps = new java.util.Properties
    system.settings.config.withOnlyPath("kafka").entrySet.asScala.foreach{e =>
      kafkaProps.put(e.getKey.substring(6), e.getValue.unwrapped)
    }
    val kafkaProducer = system.actorOf(Props(new Generator(kafkaProps)))
    val udpListener = system.actorOf(Props(new Listener(kafkaProducer)))
  }
}

class Listener(nextActor: ActorRef) extends Actor {
  import context.system
  
  val conf = context.system.settings.config
  val port = context.system.settings.config.getInt("bind.port")
  println("Starting UDP-KAFKA-BRIDGE on port $port")

  IO(Udp) ! Udp.Bind(self, new java.net.InetSocketAddress(
    conf.getString("bind.host"),
    conf.getInt("bind.port")))
 
  def receive = {
    case Udp.Bound(local)=>
      context.become(ready(sender))
  
 
  def ready(socket: ActorRef): Receive = {
    case Udp.Received(data, remote) =>
      nextActor ! data.utf8String
    case Udp.Unbind  => socket ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
  }
}

class Generator(kafkaProps: java.util.Properties) extends Actor {
  val producer = new Producer[Integer, String](new ProducerConfig(kafkaProps))
  val topicName = context.system.settings.config.getString("topic")
  val timeformat = new SimpleDateFormat("dd/MM/yyyy,HH:mm:ss")
  
  def receive = {
    case message: String => 
      // adding current date and time to UDP message
      val currentTime = timeformat.format(Calendar.getInstance().getTime())
      producer.send(new KeyedMessage[Integer, String](topicName, currentTime+";"+message))
  }
}

