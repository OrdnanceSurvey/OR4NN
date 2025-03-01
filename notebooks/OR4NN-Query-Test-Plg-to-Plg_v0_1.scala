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
// MAGIC %md #OR4NN Query Test: Polygon on Polygon

// COMMAND ----------

// MAGIC %md ###Run the OR4NN Query Library notebook

// COMMAND ----------

// MAGIC %run ./OR4NN-Query_v0_1

// COMMAND ----------

// MAGIC %md ##Data Paths
// MAGIC
// MAGIC Query dataframe should have an unique ID column of STRING type and a geometry column in Sedona UDT form (not wkt or json)
// MAGIC
// MAGIC Please refer to the PreProcess library for more infomation on the pre-processed data with Object Range information

// COMMAND ----------

// pre-processed partition with object range for green site polygons (149446 sites)
val or_path = ""
//
// query set (14767492 building polygons)
val qry_path = ""
//
// where to store result
val save_path = ""

// COMMAND ----------

// MAGIC %md ##Query Parameters

// COMMAND ----------

// K for NN
val K = 3
//
// resolution of the partition grid
val part_res = 1000.0
// column name of partition id
val part_id_nm = "cell_id"
// column name of partition geometry
val part_geom_nm = "geomPart"
//
// column name of the array of object ID+geom struct in the preprocessed data
val objects_nm = "objects"
// column name of object id in the above struct 
val obj_id_nm = "site_id"
// column name of object geometry in the abocve struct
val obj_geom_nm = "geomObj"
//
// column name of query geometry
val qry_geom_nm = "geomQry"
// column name of query id
val qry_id_nm = "qry_id"
//

// COMMAND ----------

// MAGIC %md ##Load object and query datasets and create dataframes
// MAGIC
// MAGIC Query dataframe should have an unique ID column of STRING type and a geometry column in Sedona UDT form (not wkt or json)
// MAGIC
// MAGIC Please refer to the PreProcess library for more infomation on the pre-processed data with Object Range information

// COMMAND ----------

//
val orDf = spark.read.load(or_path)
//

val qryDf = spark.read.load(qry_path).withColumn(qry_geom_nm, ST_GeomFromWKT(col("wkt"))).drop("wkt").withColumnRenamed("id", qry_id_nm)

// COMMAND ----------

// MAGIC %md #KNN Query
// MAGIC
// MAGIC Result will be saved to `save_path` in delta format and the result dataframe is returned. If `save_path` is null, the result will not be saved but the dataframe will be returned.

// COMMAND ----------

val rltDf = compKNN(K, qryDf, qry_id_nm, qry_geom_nm, orDf, part_id_nm, part_geom_nm, objects_nm, obj_id_nm, obj_geom_nm, part_res, save_path).cache()

// COMMAND ----------

rltDf.head(1)