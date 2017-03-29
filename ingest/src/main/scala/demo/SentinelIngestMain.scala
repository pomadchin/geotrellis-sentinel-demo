package demo

import java.io.File

import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample.{Bilinear, NearestNeighbor}
import geotrellis.vector.io._
import spray.json.DefaultJsonProtocol._
import org.apache.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.cassandra.CassandraInstance
import geotrellis.spark.io.index._
import geotrellis.spark.etl._
import geotrellis.spark.etl.config.EtlConf
import org.apache.spark.SparkContext
import geotrellis.spark.tiling._
import geotrellis.spark.util._
import geotrellis.spark.{ContextRDD, KeyBounds, LayerId, Metadata, SpaceTimeKey, TemporalProjectedExtent, TileLayerMetadata, TileLayerRDD}
import geotrellis.proj4.WebMercator
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerManager, FileLayerWriter}
import geotrellis.spark.pyramid.Pyramid
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.index._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.accumulo._
import geotrellis.raster.reproject._
import geotrellis.spark.tiling._
import org.apache.spark.SparkContext
import org.joda.time.DateTime

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.proj4._

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._

import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd._

import scala.io.StdIn
import java.io.File


object SentinelIngestMain {

    // Constants
  val inputPath = "file://" + new File("/home/kkaralas/Documents/vboxshare/t34tel").getAbsolutePath
  val outputPath = "data/catalog"
  val layerName = "t34tel"

  // Setup Spark to use Kryo serializer.
  val conf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Tiler")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

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

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(conf)

    try {
      val (zoom, reprojected) = run(sc)
      println(s"zoom: $zoom, reprojected: $reprojected")
      // Pause to wait to close the spark context, so that you can check out the UI at http://localhost:4040
      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }

    println("\nStarting Layer Updater")

    val files = getListOfFiles("/home/kkaralas/Documents/shared/data/t34tel")

    for(geotiff <- files) {
      println(s"Updating with GeoTiff: $geotiff")
      val sc2 = new SparkContext(conf)
      try {
        //update(sc2, geotiff.toString, zoom, reprojected)
        // Pause to wait to close the spark context, so that you can check out the UI at http://localhost:4040
        println("Hit enter to exit.")
        StdIn.readLine()
      } finally {
        sc2.stop()
      }
    }
  }

  def fullPath(path: String) = new java.io.File(path).getAbsolutePath

  def f[T](v: T) = v // get the type of a variable

  def run(implicit sc: SparkContext) : (Int, RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]) = {
    // Read the geotiff in as a single image RDD
    val firstGeoTiff = inputPath + "/test1.tif"
    val inputRdd = sc.hadoopTemporalGeoTiffRDD(firstGeoTiff)

    // Find the zoom  level that the closest match to the resolution of our source image
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd[TemporalProjectedExtent, Tile, SpaceTimeKey](inputRdd, FloatingLayoutScheme(512))

    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme
    val tilerOptions = Tiler.Options(resampleMethod = NearestNeighbor)
    val tiled = ContextRDD(inputRdd.tileToLayout[SpaceTimeKey](rasterMetaData, tilerOptions), rasterMetaData)

    // We'll be tiling the images using a zoomed layout scheme in the web mercator format
    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    // We need to reproject the tiles to WebMercator
    val (zoom, reprojected): (Int, RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]) =
      TileLayerRDD(tiled, rasterMetaData)
        .reproject(WebMercator, layoutScheme, Bilinear)

    // Create the attributes store that will tell us information about our catalog
    val attributeStore: CassandraAttributeStore = CassandraAttributeStore(instance, keyspace, attrTable)

    // Create the writer that we will use to store the tiles in the local catalog
    val writer = CassandraLayerWriter(instance, keyspace, dataTable)

    // Pyramiding up the zoom levels, write our tiles out to the Cassandra
    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
      val layerId = LayerId(layerName, z)
      // If the layer exists already, delete it out before writing
      if (attributeStore.layerExists(layerId)) {
        new CassandraLayerManager(attributeStore, instance).delete(layerId)
      }

      // let’s say we want an everyday index, but loading one tile, we have limited keyIndex space by tiles metadata information
      val keyIndex: KeyIndexMethod[SpaceTimeKey] = ZCurveKeyIndexMethod.byDay()

      // we increased in this case date time range, but you can modify anything in your “preset” key bounds
      val updatedKeyIndex = keyIndex.createIndex(rasterMetaData.bounds match {
        case kb: KeyBounds[SpaceTimeKey] => KeyBounds(
          kb.minKey.copy(instant = DateTime.parse("2015-01-01").getMillis),
          kb.maxKey.copy(instant = DateTime.parse("2020-01-01").getMillis)
        )
      })

      writer.write[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId, reprojected, updatedKeyIndex)

      if (zoom == 1) {
        // Store attributes common across zooms for catalog to see
        val id = LayerId(layerName, 0)
        attributeStore.write(id, "times",
          rdd
            .map(_._1.instant)
            .countByValue
            .keys.toArray
            .sorted)
        attributeStore.write(id, "extent",
          (rdd.metadata.extent, rdd.metadata.crs))
      }
    }

    return (zoom, reprojected)
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def update(implicit sc: SparkContext, geotiff: String, zoom: Int, reprojected: RDD[(SpaceTimeKey, Tile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]) = {
    val source = sc.hadoopTemporalGeoTiffRDD(geotiff)

    // Create the attributes store that will tell us information about our catalog
    val attributeStore: CassandraAttributeStore = CassandraAttributeStore(instance, keyspace, attrTable)

    // same steps there, to read, retile tiles
    val reader = CassandraLayerReader(instance)
    val updater: LayerUpdater[LayerId] = new CassandraLayerUpdater(instance, attributeStore, reader)

    updater.update[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](LayerId(layerName, zoom), reprojected)
    //updater.update[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](LayerId(layerName, zoom), source2)
  }

}
