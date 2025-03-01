// Databricks notebook source
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
/* OR4NN-PreProcess: functions for preprocess objects to be queried against, to facilitate efficient large scale KNN Queries 
*  
*  Author: Sheng Zhou (Sheng.Zhou@os.uk)
*
*  copyright Ordnance Survey 2023
*/


// COMMAND ----------

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.sedona.sql.utils._
import org.apache.spark._

import org.apache.sedona.core.enums._
import org.apache.sedona.core.spatialOperator._

import org.apache.spark.sql.sedona_sql.expressions.st_functions._
import org.apache.spark.sql.sedona_sql.expressions.st_constructors._
import org.apache.spark.sql.sedona_sql.expressions.st_predicates._
import org.apache.spark.sql.sedona_sql.expressions.st_aggregates._

import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable
import scala.collection.mutable._
import scala.collection.JavaConverters._
//
import uk.osgb.algorithm.or4nn.GridGenerator
import uk.osgb.algorithm.or4nn.ObjectRangeComputer
import uk.osgb.datastructures.MultiHashMap

// COMMAND ----------

/* 
*  idGeomMap: multimap of cell_id <=> row_of_object(obj_id, obj_geom)
*  cellId: cellId of the cell to find neighbours for
*  deficit: how many extra objects to be found for this cell
*  gridGen: GridGenerator instance, to use getNBCellIDs method
*  cruObjs: objects in current cell and/or added to current cell. Using set to remove potential duplicates
*  return: array of row(obj_id, obj_geom)
*/
def findObjFromNBCells(idGeomMap:MultiHashMap[String, Row], cellId:String, deficit:Int, gridGen:GridGenerator, curObjs:collection.mutable.Set[Row]):Array[Row]={
  var ringIdx:Int = 1
  var newObjs:collection.mutable.Set[Row] = collection.mutable.Set.empty[Row]
  do{
    val nbCellIds = gridGen.getNBCellIDs(cellId, ringIdx).toSeq
    for(id<-nbCellIds){
      val objs = idGeomMap.get(id)
      if(objs!=null){
        val objArray = objs.toArray
        for(obj<-objArray){
          if(!curObjs.contains(obj.asInstanceOf[Row])){
            newObjs+=obj.asInstanceOf[Row]
          }
        }  
      }    
    }
    ringIdx+=1
  }while(newObjs.size < deficit)// while there is still deficit
  newObjs.toArray
}

// COMMAND ----------

/**
* mainDf: has the following schema: cell_id:String, cell_bnd:String, geometry:udt, pc_locations:Array[struct(Postcode:String, pc_location:udt)], numLoc:Integer
*/
def findKObj(mainDf:DataFrame, lessDf:DataFrame, k:Integer, cell_id_nm:String, cell_geom_nm:String, obj_col_nm:String, num_loc_nm:String, gridGen:GridGenerator)={
  val idGeomMap = new MultiHashMap[String, Row]()
  for(row <- mainDf.select(cell_id_nm, obj_col_nm).collect()){
    val cell_id = row.getAs[String](cell_id_nm)
    val objs = row.getAs[WrappedArray[Row]](obj_col_nm)
    if(objs!=null){
      for(obj<-objs){
        idGeomMap.put(cell_id, obj)
      }
    }
  }
  val rowList = lessDf.collect()
  var newRowListBuffer:ListBuffer[Row] = new ListBuffer()
  for (row <- rowList){
    val cell_id = row.getAs[String](cell_id_nm)
    val curNumObj = row.getAs[Int](num_loc_nm)
    if(curNumObj < k){
      var curObjSet = collection.mutable.Set.empty[Row]
      var newObjs:collection.mutable.Set[Row] = collection.mutable.Set.empty[Row]
      if(curNumObj > 0){
        val curObjs = row.getAs[WrappedArray[Row]](obj_col_nm)
        for(obj<-curObjs){
          newObjs+=obj
          curObjSet+=obj
        }
      }
      // find additional objects from neighbood cells
      val nbObjs:WrappedArray[Row] = findObjFromNBCells(idGeomMap, cell_id, k - curNumObj, gridGen, curObjSet)
      for(obj<-nbObjs)
        newObjs+=obj
      // create new row and add to new row list
      if(newObjs.size < k){
        println("less than " + k + " objects found..."+curNumObj +"-"+nbObjs.size)
      }
      //val newRow = Row.fromSeq(Seq(cell_id, row.getAs[String]("cell_bnd"), row.getAs[Geometry](cell_geom_nm), newObjs.toArray, newObjs.size))
      val newRow = Row.fromSeq(Seq(cell_id, row.getAs[Geometry](cell_geom_nm), newObjs.toArray, newObjs.size))
      newRowListBuffer+=newRow
    }else{
      newRowListBuffer += row
    }
  }
  val schema = mainDf.schema
  val rl:java.util.List[Row] = newRowListBuffer.asJava
  var rltDf = spark.createDataFrame(rl,schema)
  rltDf
}

// COMMAND ----------

import uk.osgb.datastructures.MultiTreeHashMap
import scala.util.control.Breaks._

//case class Object_Cls(Postcode:String, pc_location:Geometry) // not used, we need geometry only to compute the OR
def filterObjects(qrwExt:Geometry, objects:WrappedArray[Row], obj_geom_nm:String, maxK:Int):Array[Geometry]={
  if(objects.length <= maxK){
    val rlts:Array[Geometry] = new Array[Geometry](objects.length)
    var i:Int = 0;
    for(obj<-objects){
      rlts(i) = obj.getAs[Geometry](obj_geom_nm)
      i+=1
    }
    rlts
  }
  val rlts:Array[Geometry] = new Array[Geometry](maxK)
  val cen:Point = qrwExt.getCentroid()
  // need to store objects rather than geometry as values in case of more than one object at the same location / with the same geometry
  val distIdx:MultiTreeHashMap[Double, Row] = new MultiTreeHashMap()
  for(obj<-objects){
    distIdx.put(cen.distance(obj.getAs[Geometry](obj_geom_nm)), obj)
  }
  var i:Int = 0
  breakable{
    while(i < maxK){
      try{
        val objs = distIdx.pollFirstValue()
        if(objs!=null){
          val objArray = objs.toArray
          for(obj<-objArray){
            rlts(i) = obj.asInstanceOf[Row].getAs[Geometry](obj_geom_nm)
            i+=1
            if(i==maxK)
              break
          }
        }else{
          println("no more objects...")
          break
        }
      }catch{
        case e:Exception=>println("Error filtering objects...")
      }
    }
  }
  rlts
}

// COMMAND ----------

def computeObjectRangeRect(qrwExt:Geometry, objects:WrappedArray[Row], obj_geom_nm:String, K:Int):Geometry={
  var objectRange:Geometry = null;
  try{
    val filteredObjs = filterObjects(qrwExt, objects, obj_geom_nm, K);
    objectRange = ObjectRangeComputer.compObjRangeRect(qrwExt, filteredObjs, K)
  }catch{
    case e:Exception=>println("Error in computeObjectRangeRect...\n"+e.printStackTrace)
  }
  if(objectRange== null){
    println("Error in computeObjectRangeRect...null rarnge")
    null
  }else{
    objectRange
  }
}
val computeObjectRangeRect_UDF = udf[Geometry, Geometry, WrappedArray[Row], String, Int](computeObjectRangeRect)
spark.udf.register("computeObjectRangeRect_UDF", computeObjectRangeRect_UDF)
//

// COMMAND ----------

/* objDf: obj_id:String, obj_geom:Geometry
*  partDf: part_id:String, part_geom:Geometry
*  result Dataframe: part_id:String, part_geom:Geometry, object:Array[Struct{obj_id:String, obj_geom:Geometry}]
*  for one-off process, or_path may be set to null and use the returned dataframe as input for query process directly
*/
def OR4NN_Preprocess_Sedona(objDf:DataFrame, obj_id_nm:String, obj_geom_nm:String, partDf:DataFrame, part_id_nm:String, part_geom_nm:String, K:Int, grid:GridGenerator, or_path:String)={
  val num_loc_nm = "numLoc"
  val part_id_obj_nm = part_id_nm+"_obj"
  val part_id_or_nm = part_id_nm+"_or"
  val obj_colletion_nm = "objects"
  val object_range_nm = "obj_range"

  //val considerBoundaryIntersection = true //  true to use "intersects" and false to use "contains" - Deprecated now
  val pred:SpatialPredicate = SpatialPredicate.INTERSECTS
  val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
  val usingIndex = true

  var objRDD = Adapter.toSpatialRdd(objDf, obj_geom_nm, Seq(obj_id_nm))
  //
  // may need more columnes?
  val partRDD = Adapter.toSpatialRdd(partDf.select(part_geom_nm, part_id_nm), part_geom_nm, Seq(part_id_nm))
  partRDD.analyze()
  partRDD.spatialPartitioning(GridType.QUADTREE)
  partRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
  //
  objRDD.spatialPartitioning(partRDD.getPartitioner)
  //
  //val join_rlt_obj = JoinQuery.SpatialJoinQueryFlat(objRDD, partRDD, usingIndex, considerBoundaryIntersection)
  val join_rlt_obj = JoinQuery.SpatialJoinQueryFlat(objRDD, partRDD, usingIndex, pred)
  //
  // the user data column parameters are in reversed order objRDD-gridRDD -> gridRDD_userData-objRDD_userData
  val objPartDf = (Adapter.toDf(join_rlt_obj, Seq(part_id_nm), Seq(obj_id_nm), spark)
                .withColumnRenamed("rightgeometry", obj_geom_nm)
                .withColumnRenamed(part_id_nm, part_id_obj_nm)
                .select(part_id_obj_nm, obj_id_nm, obj_geom_nm)
                .groupBy(part_id_obj_nm).agg(collect_list(struct(col(obj_id_nm), col(obj_geom_nm))).alias(obj_colletion_nm))
                .withColumn(num_loc_nm, size(col(obj_colletion_nm)))
                .drop(obj_geom_nm))
  val mainDf = (objPartDf.join(partDf, partDf(part_id_nm) === objPartDf(part_id_obj_nm), "right").drop(part_id_obj_nm)
                .select(part_id_nm, part_geom_nm, obj_colletion_nm, num_loc_nm)
                .withColumn(num_loc_nm, when(col(num_loc_nm).isNull, 0).otherwise(col(num_loc_nm)))
                )
  val lessLocDf = mainDf.filter(col(num_loc_nm) < K)
// to be joined back 
  val otherDf = mainDf.filter(col(num_loc_nm) >= K)
  //findKObj(mainDf:DataFrame, lessDf:DataFrame, k:Integer, cell_id_nm:String, cell_geom_nm:String, obj_col_nm:String, num_loc_nm:String, gridGen:GridGenerator)=
  val newLessDf = findKObj(mainDf, lessLocDf, K, part_id_nm, part_geom_nm, obj_colletion_nm, num_loc_nm, grid)
  val newMainDf = otherDf.union(newLessDf).withColumn(object_range_nm, computeObjectRangeRect_UDF(col(part_geom_nm), col(obj_colletion_nm), lit(obj_geom_nm), lit(K))).withColumnRenamed(part_id_nm, part_id_or_nm)
  //
  var orRDD = Adapter.toSpatialRdd(newMainDf.select(part_id_or_nm, object_range_nm), object_range_nm, Seq(part_id_or_nm))
  orRDD.analyze()
  orRDD.spatialPartitioning(GridType.QUADTREE)
  orRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
//
  objRDD.spatialPartitioning(orRDD.getPartitioner)
//
//  val part_obj_join = JoinQuery.SpatialJoinQueryFlat(objRDD, orRDD, usingIndex, considerBoundaryIntersection)
  val part_obj_join = JoinQuery.SpatialJoinQueryFlat(objRDD, orRDD, usingIndex, pred)
// the user data column parameters are in reversed order objRDD-gridRDD -> gridRDD_userData-objRDD_userData
  val part_obj = (Adapter.toDf(part_obj_join, Seq(part_id_or_nm), Seq(obj_id_nm), spark)
              .withColumnRenamed("rightgeometry", obj_geom_nm)
              .select(part_id_or_nm, obj_id_nm, obj_geom_nm)
              .groupBy(part_id_or_nm).agg(collect_list(struct(col(obj_id_nm), col(obj_geom_nm))).alias(obj_colletion_nm))
              .drop(obj_geom_nm)
              )
  val rltDf = part_obj.join(partDf, part_obj(part_id_or_nm) === partDf(part_id_nm), "left").select(part_id_nm, part_geom_nm, obj_colletion_nm)
  if(or_path!=null){
    rltDf.write.mode("overwrite").option("overwriteSchema", true).save(or_path)
  }
  rltDf
}

// COMMAND ----------

/* objDf: obj_id:String, obj_geom:Geometry
*  partDf: part_id:String, part_geom:Geometry
*  result Dataframe: part_id:String, part_geom:Geometry, object:Array[Struct{obj_id:String, obj_geom:Geometry}]
*  for one-off process, or_path may be set to null and use the returned dataframe as input for query process directly
*/
def OR4NN_Preprocess_RADIG(objDf:DataFrame, obj_id_nm:String, obj_geom_nm:String, partDf:DataFrame, part_id_nm:String, part_geom_nm:String, K:Int, grid:GridGenerator, or_path:String)={
  val num_loc_nm = "numLoc"
  val part_id_obj_nm = part_id_nm+"_obj"
  val part_id_or_nm = part_id_nm+"_or"
  val obj_colletion_nm = "objects"
  val object_range_nm = "obj_range"

  //val considerBoundaryIntersection = true //  true to use "intersects" and false to use "contains" - Deprecated now
  val pred:SpatialPredicate = SpatialPredicate.INTERSECTS
  val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
  val usingIndex = true

  var objRDD = Adapter.toSpatialRdd(objDf, obj_geom_nm, Seq(obj_id_nm))
  //
  // may need more columnes?
  val partRDD = Adapter.toSpatialRdd(partDf.select(part_geom_nm, part_id_nm), part_geom_nm, Seq(part_id_nm))
  partRDD.analyze()
  partRDD.spatialPartitioning(GridType.QUADTREE)
  partRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
  //
  objRDD.spatialPartitioning(partRDD.getPartitioner)
  //
  //val join_rlt_obj = JoinQuery.SpatialJoinQueryFlat(objRDD, partRDD, usingIndex, considerBoundaryIntersection)
  val join_rlt_obj = JoinQuery.SpatialJoinQueryFlat(objRDD, partRDD, usingIndex, pred)
  //
  // the user data column parameters are in reversed order objRDD-gridRDD -> gridRDD_userData-objRDD_userData
  val objPartDf = (Adapter.toDf(join_rlt_obj, Seq(part_id_nm), Seq(obj_id_nm), spark)
                .withColumnRenamed("rightgeometry", obj_geom_nm)
                .withColumnRenamed(part_id_nm, part_id_obj_nm)
                .select(part_id_obj_nm, obj_id_nm, obj_geom_nm)
                .groupBy(part_id_obj_nm).agg(collect_list(struct(col(obj_id_nm), col(obj_geom_nm))).alias(obj_colletion_nm))
                .withColumn(num_loc_nm, size(col(obj_colletion_nm)))
                .drop(obj_geom_nm))
  val mainDf = (objPartDf.join(partDf, partDf(part_id_nm) === objPartDf(part_id_obj_nm), "right").drop(part_id_obj_nm)
                .select(part_id_nm, part_geom_nm, obj_colletion_nm, num_loc_nm)
                .withColumn(num_loc_nm, when(col(num_loc_nm).isNull, 0).otherwise(col(num_loc_nm)))
                )
  val lessLocDf = mainDf.filter(col(num_loc_nm) < K)
// to be joined back 
  val otherDf = mainDf.filter(col(num_loc_nm) >= K)
  //findKObj(mainDf:DataFrame, lessDf:DataFrame, k:Integer, cell_id_nm:String, cell_geom_nm:String, obj_col_nm:String, num_loc_nm:String, gridGen:GridGenerator)=
  val newLessDf = findKObj(mainDf, lessLocDf, K, part_id_nm, part_geom_nm, obj_colletion_nm, num_loc_nm, grid)
  val newMainDf = otherDf.union(newLessDf).withColumn(object_range_nm, computeObjectRangeRect_UDF(col(part_geom_nm), col(obj_colletion_nm), lit(obj_geom_nm), lit(K))).withColumnRenamed(part_id_nm, part_id_or_nm)
  //
  var orRDD = Adapter.toSpatialRdd(newMainDf.select(part_id_or_nm, object_range_nm), object_range_nm, Seq(part_id_or_nm))
  orRDD.analyze()
  orRDD.spatialPartitioning(GridType.QUADTREE)
  orRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
//
  objRDD.spatialPartitioning(orRDD.getPartitioner)
//
//  val part_obj_join = JoinQuery.SpatialJoinQueryFlat(objRDD, orRDD, usingIndex, considerBoundaryIntersection)
  val part_obj_join = JoinQuery.SpatialJoinQueryFlat(objRDD, orRDD, usingIndex, pred)
// the user data column parameters are in reversed order objRDD-gridRDD -> gridRDD_userData-objRDD_userData
  val part_obj = (Adapter.toDf(part_obj_join, Seq(part_id_or_nm), Seq(obj_id_nm), spark)
              .withColumnRenamed("rightgeometry", obj_geom_nm)
              .select(part_id_or_nm, obj_id_nm, obj_geom_nm)
              .groupBy(part_id_or_nm).agg(collect_list(struct(col(obj_id_nm), col(obj_geom_nm))).alias(obj_colletion_nm))
              .drop(obj_geom_nm)
              )
  val rltDf = part_obj.join(partDf, part_obj(part_id_or_nm) === partDf(part_id_nm), "left").select(part_id_nm, part_geom_nm, obj_colletion_nm)
  if(or_path!=null){
    rltDf.write.mode("overwrite").option("overwriteSchema", true).save(or_path)
  }
  rltDf
}