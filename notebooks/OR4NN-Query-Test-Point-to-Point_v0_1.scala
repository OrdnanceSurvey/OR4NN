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
// MAGIC %md #OR4NN Query Test: Point on Point

// COMMAND ----------

// MAGIC %md ##Run the OR4NN Query Library notebook

// COMMAND ----------

// MAGIC %run ./OR4NN-Query_v0_1

// COMMAND ----------

// MAGIC %md ##Data Paths

// COMMAND ----------

// pre-processed partition and obj_range 
val or_path = ""
//
// query set
val query_path = ""
//
// where the result will be saved
val save_Path = ""

// COMMAND ----------

// MAGIC %md ##Query Parameters

// COMMAND ----------

//
val K = 3
//
// resolution of the partition grid
val part_res = 1000.0
//
// column names of partition id and partition geometry
val part_id_nm = "cell_id"
val part_geom_nm = "geomPart"
//
// column names of query id and query geometry
val qry_id_nm = "uprn"
val qry_geom_nm = "geomeQry"
//
// column names of object set array, object id and object geometry
val objects_nm = "objects"
val obj_id_nm = "postcode"
val obj_geom_nm = "geomObj"

// COMMAND ----------

// MAGIC %md ##Load object and query datasets and create dataframes
// MAGIC
// MAGIC
// MAGIC Query dataframe should have an unique ID column of STRING type and a geometry column in Sedona UDT form (not wkt or json)
// MAGIC
// MAGIC Please refer to the PreProcess library for more infomation on the pre-processed data with Object Range information

// COMMAND ----------

val orDf = spark.read.load(or_path)
// 
val qryDf = spark.read.load(query_path).select("uprn", "blpuComponent.geometry_BNG").filter(col("geometry_BNG").isNotNull).withColumn(qry_geom_nm, ST_GeomFromGeoJSON(col("geometry_BNG"))).drop("geometry_BNG").withColumnRenamed("uprn", "uprn_long").selectExpr("cast(uprn_long as String) uprn", qry_geom_nm).dropDuplicates("uprn")

// COMMAND ----------

// MAGIC %md #KNN Query
// MAGIC
// MAGIC Result will be saved to `save_path` in delta format and the result dataframe is returned. If `save_path` is null, the result will not be saved but the dataframe will be returned.

// COMMAND ----------

//5.56m nondet; 5.75min det
val rltDf = compKNN(K, qryDf, qry_id_nm, qry_geom_nm, orDf, part_id_nm, part_geom_nm, objects_nm, obj_id_nm, obj_geom_nm, part_res, save_Path).cache()

// COMMAND ----------

display(rltDf)