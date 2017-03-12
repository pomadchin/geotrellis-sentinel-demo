package demo

import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.hbase._

import org.apache.spark._
import org.apache.accumulo.core.client.security.tokens._
import akka.actor._
import akka.io.IO

import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer

object AkkaSystem {
  implicit val system = ActorSystem("iaas-system") // used to run the actors
  implicit val materializer = ActorMaterializer() // materialises underlying flow defintion into a set of actors

  trait LoggerExecutor {
    protected implicit val log = Logging(system, "app")
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    import AkkaSystem._

    val conf: SparkConf =
      new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("Demo Server")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    implicit val sc = new SparkContext(conf)

    val readerSet = {
      val localCatalog = args(1)
      new FileReaderSet(localCatalog)
     }

    val router = new Router(readerSet, sc)
    // Start the server - The created route is bounded to a port to start serving HTTP requests
    Http().bindAndHandle(router.routes, "0.0.0.0", 8899)
  }
}


// object Main {
//   val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ")

//   /** Usage:
//     * First argument is catalog type. Others are dependant on the first argument.
//     *
//     * local CATALOG_DIR
//     * s3 BUCKET_NAME CATALOG_KEY
//     * accumulo INSTANCE ZOOKEEPER USER PASSWORD
//     */
//   def main(args: Array[String]): Unit = {

//     // create and start our service actor
//     val service =
//       system.actorOf(Props(classOf[DemoServiceActor], readerSet, sc), "demo")

//     // start a new HTTP server on port 8899 with our service actor as the handler
//     IO(Http) ! Http.Bind(service, "0.0.0.0", 8899)
//   }
// }
