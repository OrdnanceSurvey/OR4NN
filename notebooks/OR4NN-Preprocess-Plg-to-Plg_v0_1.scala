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
// MAGIC %md #OR4NN Object Data Pre-process Test: Polygon on Polygon

// COMMAND ----------

// MAGIC %run ./OR4NN-PreProcess_v0_1

// COMMAND ----------

// MAGIC %md ##Data Paths

// COMMAND ----------

val grid_path = ""
val or_path = ""
// obj set: green site
val object_path = ""

// COMMAND ----------

// MAGIC %md ##Pre-Process Parameters

// COMMAND ----------

val K = 3
//
val obj_geom_nm = "geomObj"
val obj_id_nm = "site_id"
//
val part_geom_nm = "geomPart"
val part_id_nm = "cell_id"

val grid = GridGenerator.GB1KM

// COMMAND ----------

// MAGIC %md ##Load partition and object datasets and create dataframes

// COMMAND ----------

val dfObj = spark.read.load(object_path).withColumn(obj_geom_nm, ST_GeomFromWKT(col("wkt"))).withColumnRenamed("id", obj_id_nm).drop("wkt")
val dfPart = spark.read.load(grid_path).withColumn(part_geom_nm, ST_GeomFromWKT(col("cell_bnd"))).drop("cell_bnd")

// COMMAND ----------

// MAGIC %md ##Pre-processing data

// COMMAND ----------

val rltDf = OR4NN_Preprocess_Sedona(dfObj, obj_id_nm, obj_geom_nm, dfPart, part_id_nm, part_geom_nm, K, grid, or_path).cache()

// COMMAND ----------

display(rltDf)