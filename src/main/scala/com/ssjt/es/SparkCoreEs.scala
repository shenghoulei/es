package com.ssjt.es

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark


object SparkCoreEs {

	def main(args: Array[String]): Unit = {
		val sc = getSparkContext
		readEs(sc)
	}

	/**
	  * 获取SparkContext
	  * @return
	  */
	def getSparkContext: SparkContext = {
		val sparkConf = new SparkConf().setAppName("ES").setMaster("local[5]")
		new SparkContext(sparkConf)
	}

	// 设置操作es的参数
	val esConfig = Map("cluster.name" -> "elasticsearch",
		"es.index.auto.create" -> "true",
		"es.nodes" -> "10.10.100.16",
		"es.port" -> "9200",
		"es.index.read.missing.as.empty" -> "true",
		//"es.net.http.auth.user" -> "elastic", //访问es的用户名
		//"es.net.http.auth.pass" -> "changeme", //访问es的密码
		"es.nodes.wan.only" -> "true")

	/**
	  * 读取es的数据并处理
	  * @param sc SparkContext
	  */
	def readEs(sc: SparkContext) {
		val rdd2 = EsSpark.esJsonRDD(sc, "lagou", esConfig)
				.map(_._2).filter(_.contains("python"))
		rdd2.foreach(println)
	}

	/**
	  *  写出数据到es
	  * @param sc
	  */
	def write2Es(sc: SparkContext) = {
		val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
		val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")
		var rdd = sc.makeRDD(Seq(numbers, airports))
		EsSpark.saveToEs(rdd, "book/no")
		println("--------------------End-----------------")
	}

}
