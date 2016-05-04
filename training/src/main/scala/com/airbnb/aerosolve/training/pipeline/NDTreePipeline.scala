package com.airbnb.aerosolve.training.pipeline

import com.airbnb.aerosolve.core.{Example, FeatureMap}
import com.airbnb.aerosolve.core.util.Util
import com.airbnb.aerosolve.training.{KDTree, TrainingUtils}
import com.airbnb.aerosolve.training.KDTree.KDTreeBuildOptions
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._

object NDTreePipeline {
  val log: Logger = LoggerFactory.getLogger("NDTreePipeline")

  /*
    build NDTree from examples, each float/dense feature generates a NDTree
    and save to FeatureMap
    Sample config
    make_feature_map {
      input :  ${training_data}
      output:  ${feature_map}
      sample: 0.01
      min_count: 200
    }
   */
  def buildFeatureMapRun(sc: SparkContext, config : Config) = {
    val cfg = config.getConfig("make_feature_map")
    val inputPattern: String = cfg.getString("input")
    val sample : Double = cfg.getDouble("sample")
    val minCount : Int = config.getInt("min_count")

    log.info("Training data: %s".format(inputPattern))
    val input = GenericPipeline.getExamples(sc, inputPattern).sample(false, sample).collect()

    val floatMap = examplesToFloatFeatureArray(input)
    filter(floatMap, minCount)
    val denseMap = examplesToDenseArray(input).retain((k, v) => v.length >= minCount)
    val strMap = examplesToString(input, minCount)
    val featureMap = new FeatureMap
    featureMap.strFeatures = strMap.mapValues(_.asJava).asJava
    val output = cfg.getString("output")
    val hc = new HiveContext(sc)
    TrainingUtils.saveThrift(featureMap, output)

//    val pts : Array[(Double, Double)] = hc.sql(query).map(row => (row.getDouble(0), row.getDouble(1))).collect
//    log.info("Num listings = %d".format(pts.size))
//
//    val options = KDTreeBuildOptions(maxTreeDepth = cfg.getInt("max_depth"),
//      minLeafCount = cfg.getInt("min_leaf_count"))
//    val tree = KDTree(options, pts.toArray)
//    val nodes = tree.nodes
//    log.info("Num nodes = %d".format(nodes.size))
//    val minCount = nodes.map(node => node.count).reduce((a, b) => math.min(a,b))
//    val maxCount = nodes.map(node => node.count).reduce((a, b) => math.max(a,b))
//    log.info("Min count = %d max count = %d".format(minCount, maxCount))
  }

  def examplesToString(input: Array[Example], minCount: Int): mutable.Map[String, mutable.Buffer[String]] = {
    val map = mutable.Map[String, mutable.Map[String, Int]]()
    for (example <- input) {
      for (featureVector <- example.getExample) {
        for ((family, features) <- Util.flattenString(featureVector)) {
          for(name <- features) {
            val familyMap = map.getOrElseUpdate(family, mutable.Map[String, Int]())
            val value = familyMap.getOrElseUpdate(name, 0)
            familyMap.put(name, value + 1)
          }
        }
      }
    }

    filter(map, minCount)
  }

  def filter(map: mutable.Map[String, mutable.Map[String, Int]], minCount: Int):
      mutable.Map[String, ArrayBuffer[String]] = {
    val result = mutable.Map[String, ArrayBuffer[String]]()
    for ((family, features) <- map) {
      val outputFeatures = ArrayBuffer[String]()
      for ((feature, count) <- features) {
        if (count >= minCount) {
          outputFeatures += feature
        }
      }
      if (outputFeatures.length > 0) {
        result.put(family, outputFeatures)
      }
    }
    result
  }

  def filter[T](map: mutable.Map[String, mutable.Map[String, ArrayBuffer[T]]], minCount: Int): Unit = {
    for ((family, features) <- map) {
      features.retain((k, v) => v.length >= minCount)
    }
    map.retain((k, v) => v.size > 0)
  }

//  def filter(map: mutable.Map[String, mutable.Map[String, Int]], minCount: Int): Unit = {
//    for ((family, features) <- map) {
//      features.retain((k, v) => v >= minCount)
//    }
//    map.retain((k, v) => v.size > 0)
//  }

  def examplesToDenseArray(input: Array[Example]): mutable.Map[String, ArrayBuffer[ArrayBuffer[Double]]] = {
    val map = mutable.Map[String, ArrayBuffer[ArrayBuffer[Double]]]()
    for (example <- input) {
      for (featureVector <- example.getExample) {
        for ((name, feature) <- Util.flattenDense(featureVector)) {
          val values = map.getOrElseUpdate(name, ArrayBuffer[ArrayBuffer[Double]]())
          values += feature
        }
      }
    }
    map
  }

  /*
    map can be millions of items, so just return mutable.Map
   */
  def examplesToFloatFeatureArray(input: Array[Example]) :
      mutable.Map[String, mutable.Map[String, ArrayBuffer[Double]]] = {
    val map = mutable.Map[String, mutable.Map[String, ArrayBuffer[Double]]]()
    for (example <- input) {
      for (featureVector <- example.getExample) {
        for ((family, features) <- Util.flattenFloat(featureVector)) {
          for((name, value) <- features) {
            val familyMap = map.getOrElseUpdate(family, mutable.Map[String, ArrayBuffer[Double]]())
            val values = familyMap.getOrElseUpdate(name, ArrayBuffer[Double]())
            values += value
          }
        }
      }
    }
    map
  }

}
