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
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerWriter}
import geotrellis.spark.pyramid._
import geotrellis.spark.tiling._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import spray.json._
import spray.json.DefaultJsonProtocol._


object SentinelIngestMain extends App {

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
      .setAppName("Spark Ingest")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
  implicit val sc = new SparkContext(conf)

  val source = sc.hadoopTemporalGeoTiffRDD("/home/kkaralas/Documents/shared/data/t34tel/test1.tif")

  val (_, md) = TileLayerMetadata.fromRdd[TemporalProjectedExtent, Tile, SpaceTimeKey](source, FloatingLayoutScheme(256))

  // Keep the same number of partitions after tiling
  val tilerOptions = Tiler.Options(resampleMethod = NearestNeighbor)
  val tiled = ContextRDD(source.tileToLayout[SpaceTimeKey](md, tilerOptions), md)
  val (zoom, reprojected) = tiled.reproject(WebMercator, ZoomedLayoutScheme(WebMercator), NearestNeighbor)

  // Create the attributes store that will tell us information about our catalog
  val attributeStore = CassandraAttributeStore(instance)
  // Create the writer that we will use to store the tiles in the Cassandra catalog
  val writer = CassandraLayerWriter(attributeStore, keyspace, dataTable)

  // We want an everyday index, but loading one tile, we have limited keyIndex space by tiles metadata information
  val keyIndex: KeyIndexMethod[SpaceTimeKey] = ZCurveKeyIndexMethod.byDay()

  // We increased in this case date time range, but you can modify anything in your “preset” key bounds
  val updatedKeyIndex = keyIndex.createIndex(md.bounds match {
    case kb: KeyBounds[SpaceTimeKey] => KeyBounds(
      kb.minKey.copy(instant = DateTime.parse("2015-01-01").getMillis),
      kb.maxKey.copy(instant = DateTime.parse("2020-01-01").getMillis)
    )
  })

  // We'll be tiling the images using a zoomed layout scheme in the web mercator format
  val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

  // Pyramiding up the zoom levels, write our tiles out to Cassandra
  Pyramid.upLevels(reprojected, layoutScheme, zoom, 0, NearestNeighbor) { (rdd, z) =>
    val layerId = LayerId(layerName, z)

    // Writing a layer with larger than default keyIndex space
    writer.write[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId, reprojected, updatedKeyIndex)

    if (z == 0) {
      val id = LayerId(layerName, 0)
      attributeStore.write(id, "times",
        rdd
          .map(_._1.instant)
          .countByValue
          .keys.toArray
          .sorted)
      attributeStore.write(id, "extent",
        (md.extent, md.crs))
    }
  }

  sc.stop()
}
