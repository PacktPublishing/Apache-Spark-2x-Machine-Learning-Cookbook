package spark.ml.cookbook.chapter13

import java.io.{BufferedOutputStream, PrintWriter}
import java.net.Socket
import java.net.ServerSocket

import scala.util.Random

class CountSreamThread(socket: Socket) extends Thread {

  val villians = Array("Bane", "Thanos", "Loki", "Apocalypse", "Red Skull", "The Governor", "Sinestro", "Galactus",
    "Doctor Doom", "Lex Luthor", "Joker", "Magneto", "Darth Vader")

  override def run(): Unit = {

        println("Connection accepted")
        val out = new PrintWriter(new BufferedOutputStream(socket.getOutputStream()))

        println("Producing Data")
        while (true) {
            out.println(villians(Random.nextInt(villians.size)))
            Thread.sleep(10)
        }

        println("Done Producing")
  }
}

object CountStreamProducer {

  def main(args: Array[String]): Unit = {

      val ss = new ServerSocket(9999)
      while (true) {
        println("Accepting Connection...")
        new CountSreamThread(ss.accept()).start()
      }
  }
}