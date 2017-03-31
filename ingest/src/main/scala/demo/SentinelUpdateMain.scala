package demo

import geotrellis.proj4._
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.vector.io._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerUpdater}
import geotrellis.spark.pyramid._
import geotrellis.spark.tiling._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import spray.json._
import spray.json.DefaultJsonProtocol._


/**
  * Created by kkaralas on 3/31/17.
  */
object SentinelUpdateMain {

  val instance: CassandraInstance = new CassandraInstance {
    override val username = "cassandra"
    override val password = "cassandra"
    override val hosts = Seq("localhost")
    override val localDc = "datacenter1"
    override val replicationStrategy = "SimpleStrategy"
    override val allowRemoteDCsForLocalConsistencyLevel = false
    override val usedHostsPerRemoteDc = 0
    override val replicationFactor = 1
  }

  val keyspace: String = "geotrellis"
  val layerName = "catalog"
  val dataTable: String = layerName

  // Setup Spark to use Kryo serializer
  val conf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Update")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
  implicit val sc = new SparkContext(conf)

  val source = sc.hadoopTemporalGeoTiffRDD("/home/kkaralas/Documents/shared/data/t34tel/test2.tif")

  val (_, md) = TileLayerMetadata.fromRdd[TemporalProjectedExtent, Tile, SpaceTimeKey](source, FloatingLayoutScheme(256))

  // Keep the same number of partitions after tiling
  val tilerOptions = Tiler.Options(resampleMethod = NearestNeighbor)
  val tiled = ContextRDD(source.tileToLayout[SpaceTimeKey](md, tilerOptions), md)
  val (zoom, reprojected) = tiled.reproject(WebMercator, ZoomedLayoutScheme(WebMercator), NearestNeighbor)

  // Use the same Cassandra instance used for the first ingest
  val attributeStore = CassandraAttributeStore(instance)

  val updater = CassandraLayerUpdater(attributeStore)

  // We'll be tiling the images using a zoomed layout scheme in the web mercator format
  val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

  // Pyramiding up the zoom levels, update our tiles out to Cassandra
  Pyramid.upLevels(reprojected, layoutScheme, zoom, 0, NearestNeighbor) { (rdd, z) =>
    val layerId = LayerId(layerName, z)

    updater.update[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId, reprojected)

    if (z == 0) {
      val id = LayerId(layerName, 0)
      val times = attributeStore.read[Array[Long]](id, "times") // read times
      attributeStore.delete(id, "times") // delete it
      attributeStore.write(id, "times", // write new on the zero zoom level
        (times ++ rdd
          .map(_._1.instant)
          .countByValue
          .keys.toArray
          .sorted))

      /*val extent = attributeStore.read[Array[Long]](id, "extent")
      attributeStore.delete(id, "extent")
      attributeStore.write(id, "extent",
        (md.extent, md.crs))*/
    }
  }

  sc.stop()
}
