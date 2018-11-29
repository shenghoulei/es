package com.ssjt.es

import org.apache.spark.sql.SparkSession

object SparkSqlEs {

	def main(args: Array[String]): Unit = {
		val spark = getSparkSession
//		readEs(spark)
		writeEs(spark)
	}


	/**
	  * 设置运行环境的信息
	  *
	  * @return
	  */
	def getSparkSession: SparkSession = {
		val spark = SparkSession
				.builder()
				.appName("SparkSql Operate ES")
				.master("local[5]")
				.config("spark.some.config.option", "some-value")
				.getOrCreate()
		spark
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

	def readEs(spark: SparkSession) {
		import spark.implicits._
		val postion = spark.read.format("org.elasticsearch.spark.sql").options(esConfig).load("lagou/job")
		postion.printSchema()
		val res = postion.select("company.name")
		res.show()
	}

	//定义Person　case class
	case class Person(name: String, surname: String, age: Int)
	def writeEs(spark: SparkSession) {
		import spark.implicits._
		import org.elasticsearch.spark.sql._
		val df = spark.sparkContext
				.textFile("D:\\data\\person.txt")
        		.map(line=>{
					val fields=line.split(",")
					Person(fields(0),fields(1),fields(2).toInt)
				}).toDF()
		df.saveToEs("person/poet",esConfig)
	}


}
