package com.airbnb.aerosolve.training.pipeline

import com.airbnb.aerosolve.core.Example
import com.airbnb.aerosolve.core.util.Util
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

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
        // feature families in linear_feature should use linear
        linear_feature: ["L", "T", "L_x_T"]
      }
     */
  def buildFeatureMapRun(sc: SparkContext, config : Config) = {
    val cfg = config.getConfig("make_feature_map")
    val inputPattern: String = cfg.getString("input")
    val sample : Double = cfg.getDouble("sample")
    val minCount : Int = config.getInt("min_count")
    // minMax.filter(x => !linearFeatureFamilies.contains(x._1._1))
    val linearFeatureFamilies: java.util.List[String] = Try(config.getStringList("linear_feature"))
      .getOrElse[java.util.List[String]](List.empty.asJava)

    val linearFeatureFamiliesBC = sc.broadcast(linearFeatureFamilies)

    log.info("Training data: ${inputPattern}")
    val input = GenericPipeline.getExamples(sc, inputPattern).sample(false, sample)
    input.mapPartitions(partition => {
      val map =  mutable.Map[(String, String), ArrayBuffer[Double]]()
      val linearFeatureFamilies = linearFeatureFamiliesBC.value
      partition.foreach(examples => {
        examplesToFloatFeatureArray(examples, linearFeatureFamilies, map)
      })
      map.iterator
    })



//      .map(examples => {
//        val weights = mutable.Map[(String, String),ArrayBuffer[Double]]
//        partition.foreach(examples => {
//
//        })
//        weights
//      })
        //    log.info(s"sample load: ${input.length}")
//
//    val floatMap = examplesToFloatFeatureArray(input, linearFeatureFamilies)
//    filter(floatMap, minCount)
//    val denseMap = examplesToDenseArray(input).retain((k, v) => v.length >= minCount)
//    val strMap = examplesToString(input, minCount)
//    val featureMap = new FeatureMap
//    featureMap.strFeatures = strMap.mapValues(_.asJava).asJava
//    val output = cfg.getString("output")
//    val hc = new HiveContext(sc)
//    TrainingUtils.saveThrift(featureMap, output)

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

//  def examplesToString(input: Array[Example], minCount: Int): mutable.Map[String, mutable.Buffer[String]] = {
//    val map = mutable.Map[String, mutable.Map[String, Int]]()
//    for (example <- input) {
//      for (featureVector <- example.getExample) {
//        for ((family, features) <- Util.flattenString(featureVector)) {
//          for(name <- features) {
//            val familyMap = map.getOrElseUpdate(family, mutable.Map[String, Int]())
//            val value = familyMap.getOrElseUpdate(name, 0)
//            familyMap.put(name, value + 1)
//          }
//        }
//      }
//    }
//
//    filter(map, minCount)
//  }

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

//  def examplesToDenseArray(input: Array[Example]): mutable.Map[String, ArrayBuffer[ArrayBuffer[Double]]] = {
//    val map = mutable.Map[String, ArrayBuffer[ArrayBuffer[Double]]]()
//    for (example <- input) {
//      for (featureVector <- example.getExample) {
//        for ((name, feature) <- Util.flattenDense(featureVector)) {
//          val values = map.getOrElseUpdate(name, ArrayBuffer[ArrayBuffer[Double]]())
//          values += feature
//        }
//      }
//    }
//    map
//  }

  /*
    map can be millions of items, so just return mutable.Map
   */
  def examplesToFloatFeatureArray(
      example: Example, linearFeatureFamilies:java.util.List[String],
      map: mutable.Map[(String, String), ArrayBuffer[Double]]): Unit = {
    for (i <- 0 until example.getExample.size()) {
      val featureVector = example.getExample.get(i)
      val floatFeatures = Util.flattenFloat(featureVector).asScala
      floatFeatures.foreach(familyMap => {
        val family = familyMap._1
        if (!linearFeatureFamilies.contains(family)) {
          familyMap._2.foreach(feature => {
            val values = map.getOrElseUpdate((family, feature._1), ArrayBuffer[Double]())
            values += feature._2
          })
        }
      })
    }
  }

}
