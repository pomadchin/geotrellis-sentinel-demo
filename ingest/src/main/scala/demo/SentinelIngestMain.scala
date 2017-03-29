package demo

import java.io.File

import geotrellis.raster._
import geotrellis.raster.reproject._
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.vector.io._
import spray.json.DefaultJsonProtocol._
import org.apache.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.cassandra.CassandraInstance
import geotrellis.spark.io.index.{KeyIndexMethod, ZCurveKeyIndexMethod}
import geotrellis.spark.etl._
import geotrellis.spark.etl.config.EtlConf
import org.apache.spark.SparkContext
import geotrellis.spark.tiling._
import geotrellis.spark.util._
import geotrellis.spark.{ContextRDD, KeyBounds, LayerId, SpaceTimeKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.proj4.WebMercator
import org.joda.time.DateTime

/*object SentinelIngestMain extends App {
    implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL SinglebandIngest", new SparkConf(true))
    try {
      // multiband ingest
      //Etl.ingest[TemporalProjectedExtent, SpaceTimeKey, MultibandTile](args)
      // singleband ingest
      Etl.ingest[TemporalProjectedExtent, SpaceTimeKey, Tile](args)
    } finally {
      sc.stop()
    }
}*/

/*object SentinelIngestMain extends App {
  // create Spark configuration
  val conf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("GeoTrellis ETL SinglebandIngest")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

  implicit val sc = new SparkContext(conf) // create Spark context

  try {
    EtlConf(args) foreach { conf =>
      /* parse command line arguments */
      val etl = Etl(conf, Etl.defaultModules)
      /* load source tiles using input module specified */
      val sourceTiles = etl.load[TemporalProjectedExtent, Tile]
      /* perform the reprojection and mosaicing step to fit tiles to LayoutScheme specified */
      val (zoom, tiled) = etl.tile[TemporalProjectedExtent, Tile, SpaceTimeKey](sourceTiles)

      /* save and optionally pyramid the mosaiced layer */
      val saveAction: Etl.SaveAction[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]] =
        (attributeStore, writer, id, rdd) => {

          println(s"attributeStore=$attributeStore, writer=$writer, id=$id, rdd=$rdd")

          writer.write(id, rdd)
          if(id.zoom == 0) {
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

      etl.save[SpaceTimeKey, Tile](LayerId(etl.input.name, zoom), tiled, saveAction)
    }
  } finally {
    sc.stop()
  }
}*/

object SentinelIngestMain extends App {
  val conf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("Spark Tiler")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

  val sc: SparkContext = new SparkContext(conf)

  val source = sc.hadoopTemporalGeoTiffRDD("/home/kkaralas/Documents/vboxshare/t34tel")
  val (_, md) = TileLayerMetadata.fromRdd[TemporalProjectedExtent, Tile, SpaceTimeKey](source, FloatingLayoutScheme(256))

  // Keep the same number of partitions after tiling.
  val tilerOptions = Tiler.Options(resampleMethod = NearestNeighbor)
  val tiled = ContextRDD(source.tileToLayout[SpaceTimeKey](md, tilerOptions), md)
  val (zoom, reprojected) = tiled.reproject(WebMercator, ZoomedLayoutScheme(WebMercator), NearestNeighbor)

  val instance: CassandraInstance = new CassandraInstance {
    override val username = "cassandra"
    override val hosts = "localhost"
    override val localDc = "datacenter1"
    override val replicationStrategy = "SimpleStrategy"
    override val password = "cassandra"
    override val allowRemoteDCsForLocalConsistencyLevel = false
    override val usedHostsPerRemoteDc = 0
    override val replicationFactor = 1
  }

  val keyspace: String = "geotrellis"
  val attrTable: String = "?"
  val dataTable: String = "t34tel"

  val writer = CassandraLayerWriter(instance, keyspace, dataTable)

  // let’s say we want an everyday index, but loading one tile, we have limited keyIndex space by tiles metadata information
  val keyIndex: KeyIndexMethod[SpaceTimeKey] = ZCurveKeyIndexMethod.byDay()

  // we increased in this case date time range, but you can modify anything in your “preset” key bounds
  val updatedKeyIndex = keyIndex.createIndex(md.bounds match {
    case kb: KeyBounds[SpaceTimeKey] => KeyBounds(
      kb.minKey.copy(instant = DateTime.parse("2015-01-01").getMillis),
      kb.maxKey.copy(instant = DateTime.parse("2020-01-01").getMillis)
    )
  })

  val attributeStore: CassandraAttributeStore = CassandraAttributeStore(instance, keyspace, attrTable)

  val saveAction: Etl.SaveAction[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]] =
    (attributeStore, writer, id, rdd) => {
      val layerId = LayerId("t34tel", zoom)

      // If the layer exists already, delete it out before writing
      if(attributeStore.layerExists(layerId)) {
        new CassandraLayerManager(attributeStore, instance).delete(layerId)
      }

      // writing a layer with larger than default keyIndex space
      //writer.write[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](layerId, reprojected, updatedKeyIndex)
      writer.write(layerId, reprojected, updatedKeyIndex)
      if (id.zoom == 0) {
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

  // Update existing layer with new images
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  val files = getListOfFiles("/home/kkaralas/Documents/shared/data/t34tel")
  // iterate through list of images and update layer
  files.foreach { source2 =>
    println(source2)
    // same steps there, to read, retile tiles
    val reader = CassandraLayerReader(instance)
    val updater: LayerUpdater[LayerId] = new CassandraLayerUpdater(instance, attributeStore, reader)
    updater.update[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](LayerId("t34tel", zoom), source2)
  }
}
