package com.quantumweb.cassandra

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{to_timestamp, udf, _}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{SaveMode, DataFrame, SparkSession}

/**
  * Created by hhwang on 4/23/18.
  */
object FlightInfoApp {

    case class Flight(id: Int, year: Int, day_of_month: Int, fl_date: Timestamp, airline_id: Int, carrier: String, fl_num: Int, origin_airport_id: Int, origin: String, origin_city_name: String, origin_state_abr: String,
                      dest: String, dest_city_name: String, dest_state_abr: String, dep_time: Timestamp, arr_time: Timestamp, actual_elapsed_time: Timestamp, air_time: Timestamp, distance: Int)

    def main(args: Array[String]) {

        val spark = SparkSession
            .builder()
            .appName("SparkCassandraApp")
            .config("spark.cassandra.connection.host", "localhost")
            .config("spark.cassandra.connection.port", "9042")
            .getOrCreate()

        new FlightInfoBackup().runBasicDataSourceExample(spark)
        spark.stop()
    }
}

class FlightInfo extends Serializable {

    val shortPattern = "yyyy/MM/dd"
    val fullPattern = "yyyy/MM/dd HH:mm"

    val getBucket = (airTime: Int) => {
        airTime / 10
    }

    /** a user defined column function to format the departure timestamp string from the original data **/
    val mergeDepDateTime = (date: String, time: String) => {
        val fullDateFormatter = new SimpleDateFormat(fullPattern)
        var result = ""
        var paddding = ""
        if (time.equals("2400")) {
            result = date + " " + "00:00"
        } else if (time.length == 1) {
            paddding = "00:0"
            result = date + " " + paddding + time
        } else if (time.length == 2) {
            paddding = "00:"
            result = date + " " + paddding + time
        } else if (time.length > 2) {
            val partOne = time.substring(0, time.length - 2)
            val partTwo = time.substring(time.length - 2, time.length)
            result = date + " " + partOne + ":" + partTwo
        }
        new Timestamp(fullDateFormatter.parse(result).getTime)
    }

    /** a user defined column function to format the arrival timestamp string from the original data **/
    val mergeArrDateTime = (date: String, time: String) => {
        val shortDateFormatter = new SimpleDateFormat(shortPattern)
        val fullDateFormatter = new SimpleDateFormat(fullPattern)
        var dateTime: Timestamp = new Timestamp(new Date().getTime)
        var result = ""
        var paddding = ""
        if (time.equals("2400")) {
            dateTime = new Timestamp(DateUtils.addDays(shortDateFormatter.parse(date), 1).getTime)
        } else if (time.length == 1) {
            paddding = "00:0"
            result = date + " " + paddding + time
            dateTime = new Timestamp(fullDateFormatter.parse(result).getTime)
        } else if (time.length == 2) {
            paddding = "00:"
            result = date + " " + paddding + time
            dateTime = new Timestamp(fullDateFormatter.parse(result).getTime)
        } else if (time.length > 2) {
            val partOne = time.substring(0, time.length - 2)
            val partTwo = time.substring(time.length - 2, time.length)
            result = date + " " + partOne + ":" + partTwo
            dateTime = new Timestamp(fullDateFormatter.parse(result).getTime)
        }
        dateTime
    }


    def runBasicDataSourceExample(spark: SparkSession): Unit = {

        val flightData = spark.read.schema(StructType(getDBFileds())).option("header", "true").csv("src/main/resources/flights_from_pg.csv")
        val mergeDepDateUDF = udf(mergeDepDateTime)
        val mergeArrDateUDF = udf(mergeArrDateTime)

        val flightInfo = flightData.withColumn("fl_date", to_timestamp(col("str_fl_date"), shortPattern))
            .withColumn("dep_time", mergeDepDateUDF(col("str_fl_date"), col("dep_time")))
            .withColumn("arr_time", mergeArrDateUDF(col("str_fl_date"), col("arr_time")))
            .drop("str_fl_date")

        flightInfo.write
            .cassandraFormat("flights", "exercise", "Test Cluster")
            .save()

        flightInfo.write
            .cassandraFormat("flights_departure", "exercise", "Test Cluster")
            .save()


        createAirTimeBuckets(flightInfo)

        val transformedFlightInfo = transformAirportCode(flightInfo,"BOS","TST")
        transformedFlightInfo.write
            .cassandraFormat("flights_departure", "exercise", "Test Cluster")
                .mode(SaveMode.Append).save()
    }

    /** Transform airport code from oldCode to newCode **/

    def transformAirportCode(flightInfo: DataFrame, oldCode:String, newCode:String): DataFrame = {
        // use printout to verify the correctness
        flightInfo.foreach(x => {
            if (x.getAs[String]("origin").equals(oldCode)) {
                println("Origin:" + x)
            }
        })

        flightInfo.foreach(x => {
            if (x.getAs[String]("dest").equals(oldCode)) {
                println("Dest:" + x)
            }
        })
        // transformation
        val newFligthInfo = flightInfo.withColumn("origin", when(col("origin").equalTo(oldCode), newCode).otherwise(col("origin")))
            .withColumn("dest", when(col("dest").equalTo(oldCode), newCode).otherwise(col("dest")))

        newFligthInfo.foreach(x => {
            if (x.getAs[String]("origin").equals(newCode)) {
                println("Changed Origin:" + x)
            }
        })

        newFligthInfo.foreach(x => {
            if (x.getAs[String]("dest").equals(newCode)) {
                println("Changed Dest:" + x)
            }
        })
        newFligthInfo
    }

    def createAirTimeBuckets(flightInfo: DataFrame): Unit = {
        val getBucketUDF = udf(getBucket)
        val flightAirtimeBucketInfo = flightInfo.select(col("carrier"), col("origin"), col("dest"), col("air_time"), col("id")).withColumn("bucket", getBucketUDF(col("air_time")))


        flightAirtimeBucketInfo.write
            .cassandraFormat("flights_airtime_buckets", "exercise", "Test Cluster")
            .save()


    }


    def getDBFileds(): Array[StructField] = {
        val id = StructField("id", DataTypes.IntegerType, false)
        val year = StructField("year", DataTypes.IntegerType)
        val day_of_month = StructField("day_of_month", DataTypes.IntegerType)
        val fl_date = StructField("str_fl_date", DataTypes.StringType)
        val airline_id = StructField("airline_id", DataTypes.IntegerType)
        val carrier = StructField("carrier", DataTypes.StringType)
        val fl_num = StructField("fl_num", DataTypes.IntegerType)
        val origin_airport_id = StructField("origin_airport_id", DataTypes.IntegerType)
        val origin = StructField("origin", DataTypes.StringType)
        val origin_city_name = StructField("origin_city_name", DataTypes.StringType)
        val origin_state_abr = StructField("origin_state_abr", DataTypes.StringType)
        val dest = StructField("dest", DataTypes.StringType)
        val dest_city_name = StructField("dest_city_name", DataTypes.StringType)
        val dest_state_abr = StructField("dest_state_abr", DataTypes.StringType)
        val dep_time = StructField("dep_time", DataTypes.StringType)
        val arr_time = StructField("arr_time", DataTypes.StringType)
        val actual_elapsed_time = StructField("actual_elapsed_time", DataTypes.IntegerType)
        val air_time = StructField("air_time", DataTypes.IntegerType)
        val distance = StructField("distance", DataTypes.IntegerType)

        Array(id, year, day_of_month, fl_date, airline_id, carrier, fl_num, origin_airport_id, origin, origin_city_name,
            origin_state_abr, dest, dest_city_name, dest_state_abr, dep_time, arr_time, actual_elapsed_time, air_time, distance
        )
    }
}



