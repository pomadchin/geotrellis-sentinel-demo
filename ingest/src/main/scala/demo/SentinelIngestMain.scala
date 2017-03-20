package demo

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.etl.Etl
import geotrellis.spark.util._

import org.apache.spark._

object SentinelIngestMain extends App {
    implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL SinglebandIngest", new SparkConf(true))
    try {
      // multiband ingest
      //Etl.ingest[TemporalProjectedExtent, SpaceTimeKey, MultibandTile](args)
      // singleband ingest
      Etl.ingest[TemporalProjectedExtent, SpaceTimeKey, Tile](args)
    } finally {
      sc.stop()
    }
}
