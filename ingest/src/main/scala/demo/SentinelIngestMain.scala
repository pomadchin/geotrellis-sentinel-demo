package demo

import geotrellis.vector.io._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.etl._
import geotrellis.spark.etl.config.EtlConf
import geotrellis.spark.util._

import spray.json.DefaultJsonProtocol._
import org.apache.spark._

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

object SentinelIngestMain extends App {
  implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL SinglebandIngest", new SparkConf(true))
  try {
    EtlConf(args) foreach { conf =>
      val etl = Etl(conf, Etl.defaultModules)
      val sourceTiles = etl.load[TemporalProjectedExtent, Tile]
      val (zoom, tiled) = etl.tile[TemporalProjectedExtent, Tile, SpaceTimeKey](sourceTiles)

      val saveAction: Etl.SaveAction[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]] =
        (attributeStore, writer, id, rdd) => {
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
}
