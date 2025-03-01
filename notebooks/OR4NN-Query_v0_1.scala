/**
 * Author: Sheng Zhou (Sheng.Zhou@os.uk)
 *
 * version 1.0
 *
 * Date: 2025-03-01
 *
 * Copyright (C) 2025 Ordnance Survey
 *
 * Licensed under the Open Government Licence v3.0 (the "License");
 *
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *     http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Databricks notebook source
// MAGIC %md #Parallel K-NN Query using OR4NN Pre-procseed Data

// COMMAND ----------

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.sedona.sql.utils._
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._


import org.apache.sedona.core.enums._
import org.apache.sedona.core.spatialOperator._

import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.sedona_sql.expressions.st_constructors._
import org.apache.spark.sql.sedona_sql.expressions.st_predicates._
import org.apache.spark.sql.sedona_sql.expressions.st_aggregates._

import org.locationtech.jts.geom._
//
import uk.osgb.algorithm.or4nn.KNNItemDistance
import org.locationtech.jts.index.strtree.STRtree
import scala.collection.mutable
import scala.collection.mutable._
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %run ./Radig2_v0_1

// COMMAND ----------

/* compute KNN for queries in a partition
*/
case class knn_rlt(qry_id:String, qry_geom:Geometry, rank:Int, dist:Double, obj_id:String)

def compKNNPartition(queries:WrappedArray[Row], qry_id_nm:String, qry_geom_nm:String, objects:WrappedArray[Row], obj_id_nm:String, obj_geom_nm:String, k:Int):Array[knn_rlt]={
  val sidx:STRtree = new STRtree(4);
  for(row<-objects){
    val obj_id = row.getAs[String](obj_id_nm)
    val obj_geom = row.getAs[Geometry](obj_geom_nm)
    obj_geom.setUserData(obj_id)
    sidx.insert(obj_geom.getEnvelopeInternal(), obj_geom)
  }
  var newRowListBuffer:ListBuffer[knn_rlt] = new ListBuffer()
  val itemDist = new KNNItemDistance()
  for(row<-queries){
    val qry_id = row.getAs[String](qry_id_nm)
    val qry_geom = row.getAs[Geometry](qry_geom_nm)
    qry_geom.setUserData(qry_id)
    val nbs = sidx.nearestNeighbour(qry_geom.getEnvelopeInternal(), qry_geom, itemDist, k) // return Object[]
    var rank:Int = 1
    for(nb<-nbs){
      val nbGeom = nb.asInstanceOf[Geometry]
      val obj_id = nbGeom.getUserData().asInstanceOf[String]
      val dist = qry_geom.distance(nbGeom)
      val rlt = new knn_rlt(qry_id, qry_geom, rank, dist, obj_id)
      rank+=1
      newRowListBuffer+=rlt
    }
  }
  newRowListBuffer.toArray
}
// it doesn't make much difference to make the udf non deterministic here, although it does in other occassions
//val compKNNPartition_UDF = udf[Array[knn_rlt], WrappedArray[Row], String, String, WrappedArray[Row], String, String, Int](compKNNPartition).asNondeterministic()
val compKNNPartition_UDF = udf[Array[knn_rlt], WrappedArray[Row], String, String, WrappedArray[Row], String, String, Int](compKNNPartition)
spark.udf.register("compKNNPartition_UDF", compKNNPartition_UDF)

// COMMAND ----------

/*
// assuming partition not indexed by RADIG yet. if the grid partition conforms with any level of Radig grid, Radig reference may be used as the partition id.
*/
def compKNN(K:Int, qryDf:DataFrame, qry_id_nm:String, qry_geom_nm:String, orDf:DataFrame, part_id_nm:String, part_geom_nm:String, objects_nm:String, obj_id_nm:String, obj_geom_nm:String, part_res:Double, save_Path:String)={
  val part_id_qry = "part_id_qry"
  val query_set_nm = "queries"
  val dfOr = orDf.withColumn("RADIG_GRID_A", compBNGRadigResInt_UDF(col(part_geom_nm), lit(true), lit(part_res), lit(true), lit(1))).withColumn("RADIG_GRID", explode(col("RADIG_GRID_A"))).drop("RADIG_GRID_A")
  // explode triggers null_pointer exception but explode_outer doesn't
  //val dfQry = qryDf.withColumn("RADIG_QRY_A", compPointBNGRadigRes_UDF(col(qry_geom_nm), lit(true), lit(part_res))).withColumn("RADIG_QRY", explode_outer(col("RADIG_QRY_A"))).drop("RADIG_QRY_A")
  val dfQry = qryDf.withColumn("RADIG_QRY_A", compBNGRadigResInt_UDF(col(qry_geom_nm), lit(true), lit(part_res), lit(true), lit(1))).withColumn("RADIG_QRY", explode_outer(col("RADIG_QRY_A"))).drop("RADIG_QRY_A")
  // assign partition IDs to queries
  val dfJoined = dfQry.join(dfOr, dfQry("RADIG_QRY") === dfOr("RADIG_GRID")).select(qry_id_nm, part_id_nm, qry_geom_nm)
  // group queries by partition ID
  val dfJoined2 = dfJoined.withColumnRenamed(part_id_nm, part_id_qry).groupBy(part_id_qry).agg(collect_list(struct(col(qry_id_nm), col(qry_geom_nm))).alias(query_set_nm))
  // join queries and objects for the same partition
  val dfWorking = dfJoined2.join(dfOr,  dfJoined2(part_id_qry) === dfOr(part_id_nm), "left").drop(part_id_qry, "RADIG_GRID", part_geom_nm).repartition(512, col(part_id_nm))
  //
  val dfKnn = dfWorking.withColumn("knn", compKNNPartition_UDF(col(query_set_nm), lit(qry_id_nm), lit(qry_geom_nm), col(objects_nm), lit(obj_id_nm), lit(obj_geom_nm), lit(K)))
  //
  // flat the KNN array for each query
  val dfKnn2 = dfKnn.withColumn("knn_e", explode(col("knn"))).select("knn_e")
  // generate individual columns
  val dfKnn3 = (dfKnn2.withColumn(qry_id_nm, col("knn_e").getField("qry_id"))
              .withColumn(qry_geom_nm, col("knn_e").getField("qry_geom"))
              .withColumn("rank", col("knn_e").getField("rank"))
              .withColumn("dist", col("knn_e").getField("dist"))
              .withColumn(obj_id_nm, col("knn_e").getField("obj_id"))
              .drop("knn_e").dropDuplicates(qry_id_nm, obj_id_nm)
              )
  //may drop "rank" as well, keep now for cross check
  val dfKnn4 = dfKnn3.withColumn("dist_rank", row_number().over(Window.partitionBy(qry_id_nm).orderBy($"dist".asc))).filter((col("dist_rank") <= K)).drop("rank")
  //display(dfKnn4)
  if(save_Path!=null){
    println("Saving results...")
    dfKnn4.write.mode("overwrite").option("overwriteSchema", true).save(save_Path)
  }
  dfKnn4
}