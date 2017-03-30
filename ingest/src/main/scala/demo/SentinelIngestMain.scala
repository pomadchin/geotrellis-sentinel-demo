package demo

import java.io.File

import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.vector._
import geotrellis.vector.Extent
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.io.hadoop._
import geotrellis.raster.reproject._
import geotrellis.spark.pyramid._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.tiling._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import spray.json._
import spray.json.DefaultJsonProtocol._
import geotrellis.raster.reproject._
import geotrellis.proj4._

import geotrellis.vector.io._
import geotrellis.spark.etl._
import geotrellis.spark.etl.config.EtlConf
import geotrellis.spark.util._

import scala.io.StdIn


object SentinelIngestMain extends App {

  val layerName = "test"

  val instance: CassandraInstance = new CassandraInstance {
    override val username = "cassandra"
    override val hosts = Seq("localhost")
    override val localDc = "datacenter1"
    override val replicationStrategy = "SimpleStrategy"
    override val password = "cassandra"
    override val allowRemoteDCsForLocalConsistencyLevel = false
    override val usedHostsPerRemoteDc = 0
    override val replicationFactor = 1
  }

  val keyspace: String = "geotrellis"
  val attrTable: String = "aaa"
  val dataTable: String = layerName

  // Setup Spark to use Kryo serializer
  val conf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Ingest")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
  implicit val sc = new SparkContext(conf)

  val source = sc.hadoopTemporalGeoTiffRDD("/home/kkaralas/Documents/shared/data/t34tel/test1.tif")

  val (_, md) = TileLayerMetadata.fromRdd[TemporalProjectedExtent, Tile, SpaceTimeKey](source, FloatingLayoutScheme(256))
  // Keep the same number of partitions after tiling.
  val tilerOptions = Tiler.Options(resampleMethod = NearestNeighbor)
  val tiled = ContextRDD(source.tileToLayout[SpaceTimeKey](md, tilerOptions), md)
  val (zoom, reprojected) = tiled.reproject(WebMercator, ZoomedLayoutScheme(WebMercator), NearestNeighbor)
  val attributeStore: AttributeStore = CassandraAttributeStore(instance, keyspace, attrTable)
  val writer: LayerWriter[LayerId] = CassandraLayerWriter(instance, keyspace, dataTable)
  // let’s say we want an everyday index, but loading one tile, we have limited keyIndex space by tiles metadata information
  val keyIndex: KeyIndexMethod[SpaceTimeKey] = ZCurveKeyIndexMethod.byDay()

  // we increased in this case date time range, but you can modify anything in your “preset” key bounds
  val updatedKeyIndex = keyIndex.createIndex(md.bounds match {
    case kb: KeyBounds[SpaceTimeKey] => KeyBounds(
      kb.minKey.copy(instant = DateTime.parse("2015-01-01").getMillis),
      kb.maxKey.copy(instant = DateTime.parse("2020-01-01").getMillis)
    )
  })

  val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

  Pyramid.upLevels(reprojected, layoutScheme, zoom, 0, NearestNeighbor) { (rdd, z) =>
    println(s"\n\nIn zoom $z!\n\n")
    val layerId = LayerId(layerName, z)
    // writing a layer with larger than default keyIndex space
    writer.write[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId, reprojected, updatedKeyIndex)

    if (z == 0) {
      println(s"\n\nIn zoom $z!!\n\n")

      val id = LayerId(layerName, 0)
      attributeStore.write(id, "times",
        rdd
          .map(_._1.instant)
          .countByValue
          .keys.toArray
          .sorted)
      attributeStore.write(id, "extent",
        (md.extent, md.crs))

      //val meta = attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](id)
      //println(s"\n\n$meta")
    }
  }

  sc.stop()

  // Updater

  val conf2 =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Update")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

  // now we can just update layer
  val sc2 = new SparkContext(conf)
  val source2 = sc2.hadoopTemporalGeoTiffRDD("/home/kkaralas/Documents/shared/data/t34tel/test2.tif")

  val (_, md2) = TileLayerMetadata.fromRdd[TemporalProjectedExtent, Tile, SpaceTimeKey](source2, FloatingLayoutScheme(256))
  // Keep the same number of partitions after tiling.
  val tiled2 = ContextRDD(source2.tileToLayout[SpaceTimeKey](md2, tilerOptions), md2)
  val (zoom2, reprojected2) = tiled2.reproject(WebMercator, ZoomedLayoutScheme(WebMercator), NearestNeighbor)

  // same steps there, to read, retile tiles
  val reader = CassandraLayerReader(instance)
  val updater: LayerUpdater[LayerId] = new CassandraLayerUpdater(instance, attributeStore, reader)

  Pyramid.upLevels(reprojected, layoutScheme, zoom, 0, NearestNeighbor) { (rdd, z) =>
    updater.update[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](LayerId(layerName, zoom2), reprojected2)
    //updater.update[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](LayerId("test", zoom), source2)

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

      val extent = attributeStore.read[Array[Long]](id, "extent")
      attributeStore.delete(id, "extent")
      attributeStore.write(id, "extent",
        (md2.combine(md)))
    }
  }

  sc2.stop()
}
