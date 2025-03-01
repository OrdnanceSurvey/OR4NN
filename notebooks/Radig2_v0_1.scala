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
// MAGIC %md #RADIG2

// COMMAND ----------



// COMMAND ----------

import uk.osgb.algorithm.radig2.Radig_BNG
import uk.osgb.algorithm.radig2.Radig_DigitInterleaver
//
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.serializer.KryoSerializer
//
import org.apache.sedona.sql.utils._
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.core.enums._
import org.apache.sedona.core.spatialOperator._
//
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.io.geojson.GeoJsonReader //
//import org.wololo.jts2geojson.{GeoJSONReader, GeoJSONWriter}

// COMMAND ----------

def wkt2Geom = (geomWkt:String)=>{
  val reader = new WKTReader(new GeometryFactory)
  reader.read(geomWkt)
}
// convert a GEOJSON string to WKT string
def geojson2Wkt = (geomJson: String) =>{
  val reader = new GeoJsonReader() // JTS 
  //val reader = new GeoJSONReader()
  val geometry:Geometry = reader.read(geomJson)
  geometry.toText()
}
def geojson2Geom (geomJson:String):(Geometry) = {
  val reader = new GeoJsonReader() // JTS 
  //val reader = new GeoJSONReader()
  reader.read(geomJson)
}
val geojson2WktUDF = udf(geojson2Wkt)

val saveAsWKT = (geometry: Geometry) =>{
  geometry.toText()
}
val ST_SaveAsWKT_UDF = udf(saveAsWKT)

// COMMAND ----------

// MAGIC %md #RADIG2 API 
// MAGIC ##- for generating generic RADIG references with customised radix and divisors

// COMMAND ----------

// to be complete

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #RADIG2 BNG API
// MAGIC
// MAGIC ## - for generating BNG-style RADIG references

// COMMAND ----------

/***************************
*
* utility
*
***************************/
// calc the cell size (resolution) from a BNGRadig reference string
def calcResBNGRadig(bngRadigRef:String):Double={
  Radig_BNG.calcResBNGRadig(bngRadigRef)
}
val calcResBNGRadig_UDF = udf[Double, String](calcResBNGRadig) // necessary?
// convert a BNGRadig reference to the corresponding grid cell genetry
def bngRadigRefToWKT(bngRadigRef:String):String={
	val iLRef:String = Radig_BNG.convertBNGRadigToILRef(bngRadigRef);
	val geom:Geometry = Radig_DigitInterleaver.iLRefToGeometry(iLRef, 10, 7, 2, true);
    geom.toText();
}
val bngRadigRefToWKT_UDF = udf[String, String](bngRadigRefToWKT) // necessary?

// COMMAND ----------

/***************************
*
* resolution determined by geometry extent / divisor, no subdivision
*
* -Int suffix: Whether the input geometry contains a cell is NOT tested
* -Cnt suffix: Whether the input geometry contains a cell is tested and two arrays of reference strings are returned (first being cells intersecting input geometry boundary and the second beying cells * contained by the input geometry)
* -useLowerBound: if true, the resolution in the resolution bound array that is smaller than or equals to the provisional resolution (given or derived from extent) is used
* -intPolicy: 0 for any intersection; 1 for requiring intersection of interiors
* -hasDualDivisor: if true, dual divisor system is used
***************************///
// Column is Geometry udt
// Cell resolution is determined by the extent of the input geometry (divided by the extentDivisor for extra control).
//
def compBNGRadigExtInt (geom:Geometry, extentDivisor:Int, useLowerBound:Boolean, hasDualDivisor:Boolean, intPolicy:Int):Array[String] ={
  Radig_BNG.compBNGRadigExtInt(geom, hasDualDivisor, extentDivisor, useLowerBound, intPolicy)
}
val compBNGRadigExtInt_UDF = udf[Array[String], Geometry, Int, Boolean, Boolean, Int](compBNGRadigExtInt)
spark.udf.register("compBNGRadigExtInt_UDF", compBNGRadigExtInt_UDF)
//
// geometry column is GeoJSon string
//
def compBNGRadigExtIntJSon (geomJSon:String, extentDivisor:Int, useLowerBound:Boolean, hasDualDivisor:Boolean, intPolicy:Int):Array[String] ={
  val geom:Geometry = geojson2Geom(geomJSon)
  Radig_BNG.compBNGRadigExtInt(geom, hasDualDivisor, extentDivisor, useLowerBound, intPolicy)
}
val compBNGRadigExtIntJSon_UDF = udf[Array[String], String, Int, Boolean, Boolean, Int](compBNGRadigExtIntJSon)
spark.udf.register("compBNGRadigExtIntJSon_UDF", compBNGRadigExtIntJSon_UDF)
//
// check for intersects and contains;the geometry column is Geometry udt; return two arrays of references, first for intersects, second for contains
//
def compBNGRadigExtCnt (geom:Geometry, extentDivisor:Int, useLowerBound:Boolean, hasDualDivisor:Boolean, intPolicy:Int):Array[Array[String]] ={
  Radig_BNG.compBNGRadigExtCnt(geom, hasDualDivisor, extentDivisor, useLowerBound, intPolicy)
}
val compBNGRadigExtCnt_UDF = udf[Array[Array[String]], Geometry, Int, Boolean, Boolean, Int](compBNGRadigExtCnt)
spark.udf.register("compBNGRadigExtCnt_UDF", compBNGRadigExtCnt_UDF)
//
//  check for intersects and contains; the geometry column is GeoJSon string
//
def compBNGRadigExtCntJSon (geomJSon:String, extentDivisor:Int, useLowerBound:Boolean, hasDualDivisor:Boolean, intPolicy:Int):Array[Array[String]] ={
  val geom:Geometry = geojson2Geom(geomJSon)
  Radig_BNG.compBNGRadigExtCnt(geom, hasDualDivisor, extentDivisor, useLowerBound, intPolicy)
}
val compBNGRadigExtCntJSon_UDF = udf[Array[Array[String]], String, Int, Boolean, Boolean, Int](compBNGRadigExtCntJSon)
spark.udf.register("compBNGRadigExtCntJSon_UDF", compBNGRadigExtCntJSon_UDF)

// COMMAND ----------

/***************************
*
* Resolution supplied by user - no grid subdivision
*
* -Int suffix: Whether the input geometry contains a cell is NOT tested
* -Cnt suffix: Whether the input geometry contains a cell is tested and two arrays of reference strings are returned (first being cells intersecting input geometry boundary and the second beying cells * contained by the input geometry)
* -useLowerBound: if true, the resolution in the resolution bound array that is smaller than or equals to the provisional resolution (given or derived from extent) is used
* -intPolicy: 0 for any intersection; 1 for requiring intersection of interiors
* -hasDualDivisor: if true, dual divisor system is used
*
***************************/
def compBNGRadigResInt (geom:Geometry, hasDualDivisor:Boolean, resolution:Double, useLowerBound:Boolean, intPolicy:Int):Array[String] ={
  Radig_BNG.compBNGRadigResInt(geom, hasDualDivisor, resolution, useLowerBound, intPolicy)
}
val compBNGRadigResInt_UDF = udf[Array[String], Geometry, Boolean, Double, Boolean, Int](compBNGRadigResInt)
spark.udf.register("compBNGRadigResInt_UDF", compBNGRadigResInt_UDF)
// Res based - GeoJson string
def compBNGRadigResIntJSon (geomJSon:String, hasDualDivisor:Boolean, resolution:Double, useLowerBound:Boolean,  intPolicy:Int):Array[String] ={
  val geom:Geometry = geojson2Geom(geomJSon)  
  Radig_BNG.compBNGRadigResInt(geom, hasDualDivisor, resolution, useLowerBound, intPolicy)
}
val compBNGRadigResIntJSon_UDF = udf[Array[String], String, Boolean, Double, Boolean, Int](compBNGRadigResIntJSon)
spark.udf.register("compBNGRadigResIntJSon_UDF", compBNGRadigResIntJSon_UDF)
//
def compBNGRadigResCnt (geom:Geometry, hasDualDivisor:Boolean, resolution:Double, useLowerBound:Boolean, intPolicy:Int):Array[Array[String]] ={
  Radig_BNG.compBNGRadigResCnt(geom, hasDualDivisor, resolution, useLowerBound, intPolicy)
}
val compBNGRadigResCnt_UDF = udf[Array[Array[String]], Geometry, Boolean,  Double, Boolean, Int](compBNGRadigResCnt)
spark.udf.register("compBNGRadigResCnt_UDF", compBNGRadigResCnt_UDF)
// Res based - GeoJson string
def compBNGRadigResCntJSon (geomJSon:String, hasDualDivisor:Boolean, resolution:Double, useLowerBound:Boolean,  intPolicy:Int):Array[Array[String]] ={
  val geom:Geometry = geojson2Geom(geomJSon)  
  Radig_BNG.compBNGRadigResCnt(geom, hasDualDivisor, resolution, useLowerBound, intPolicy)
}
val compBNGRadigResCntJSon_UDF = udf[Array[Array[String]], String, Boolean, Double, Boolean, Int](compBNGRadigResCntJSon)
spark.udf.register("compBNGRadigResCntJSon_UDF", compBNGRadigResCntJSon_UDF)
//
// for point feature encoding (return 1 to 4 references)
//
def compPointBNGRadigRes(point:Point, hasDualDivisor:Boolean, resolution:Double):Array[String]={
  Radig_BNG.compPointBNGRadigRefRes(point, hasDualDivisor, resolution)
}
val compPointBNGRadigRes_UDF = udf[Array[String], Point, Boolean, Double](compPointBNGRadigRes)
spark.udf.register("compPointBNGRadigRes_UDF", compPointBNGRadigRes_UDF)
//
// for point feature, return only one refernce even if it is on grid boundary
//
def compPointBNGRadigResSingle(point:Point, hasDualDivisor:Boolean, resolution:Double):String={
  Radig_BNG.compPointBNGRadigRefSingle(point, hasDualDivisor, resolution)
}
val compPointBNGRadigResSingle_UDF = udf[String, Point, Boolean, Double](compPointBNGRadigResSingle)
spark.udf.register("compPointBNGRadigResSingle_UDF", compPointBNGRadigResSingle_UDF)


// COMMAND ----------

/***************************
*
* resolution controlled by the supplied number of digits to be used for encoding (count from top bit, with padding), no subdivision
*
***************************/
def compBNGRadigDigitsInt (geom:Geometry, hasDualDivisor:Boolean, numDigits:Int, useLowerBound:Boolean, endAtFirstDivisor:Boolean, intPolicy:Int):Array[String] ={
  Radig_BNG.compBNGRadigDigitsInt(geom, hasDualDivisor, numDigits, useLowerBound, endAtFirstDivisor, intPolicy)
}
val compBNGRadigDigitsInt_UDF = udf[Array[String], Geometry, Boolean,  Int, Boolean, Boolean, Int](compBNGRadigDigitsInt)
spark.udf.register("compBNGRadigDigitsInt_UDF", compBNGRadigDigitsInt_UDF)
//
def compBNGRadigDigitsIntJSon (geomJSon:String, hasDualDivisor:Boolean, numDigits:Int, useLowerBound:Boolean, endAtFirstDivisor:Boolean, intPolicy:Int):Array[String] ={
  val geom:Geometry = geojson2Geom(geomJSon)  
  Radig_BNG.compBNGRadigDigitsInt(geom, hasDualDivisor, numDigits, useLowerBound, endAtFirstDivisor, intPolicy)
}
val compBNGRadigDigitsIntJSon_UDF = udf[Array[String], String, Boolean, Int, Boolean, Boolean, Int](compBNGRadigDigitsIntJSon)
spark.udf.register("compBNGRadigDigitsIntJSon_UDF", compBNGRadigDigitsIntJSon_UDF)
//
def compBNGRadigDigitsCnt (geom:Geometry, hasDualDivisor:Boolean, numDigits:Int, useLowerBound:Boolean, endAtFirstDivisor:Boolean, intPolicy:Int):Array[Array[String]] ={
  Radig_BNG.compBNGRadigDigitsCnt(geom, hasDualDivisor, numDigits, useLowerBound, endAtFirstDivisor, intPolicy)
}
val compBNGRadigDigitsCnt_UDF = udf[Array[Array[String]], Geometry, Boolean,  Int, Boolean, Boolean, Int](compBNGRadigDigitsCnt)
spark.udf.register("compBNGRadigDigitsCnt_UDF",compBNGRadigDigitsCnt_UDF)
//
def compBNGRadigDigitsCntJSon (geomJSon:String, hasDualDivisor:Boolean, numDigits:Int, useLowerBound:Boolean, endAtFirstDivisor:Boolean, intPolicy:Int):Array[Array[String]] ={
  val geom:Geometry = geojson2Geom(geomJSon)  
  Radig_BNG.compBNGRadigDigitsCnt(geom, hasDualDivisor, numDigits, useLowerBound, endAtFirstDivisor, intPolicy)
}
val compBNGRadigDigitsCntJSon_UDF = udf[Array[Array[String]], String, Boolean, Int, Boolean, Boolean, Int](compBNGRadigDigitsCntJSon)
spark.udf.register("compBNGRadigDigitsCntJSon_UDF", compBNGRadigDigitsCntJSon_UDF)


// COMMAND ----------

/***************************
*
* adaptive with subdivision
*
* maxDivLevel: maximum number of sub-divisions to be performed 
* minDivRes: if current cell resoluion equals to or is smaller than minDivRes, sub-division should not be be performed even if the maximum number of sub-divisions has not be reached.
* eaThreshold: for polygon geoemtry, if the ration of intersection area between the polgyon and the cell over the area of the cell is smaller tahn this threshold, the cell will become subdivisible (to * increase space utilisation)
* elThreshold: for linear geometry, similiar to eaThreshold
*
***************************/
def compBNGRadigAdaptiveInt(geom:Geometry, hasDualDivisor:Boolean, intPolicy:Int, maxDivLevel:Int, minDivRes:Double, eaThreshold:Double, elThreshold:Double):Array[String]={
  Radig_BNG.compBNGRadigAdaptiveInt(geom, hasDualDivisor, intPolicy, maxDivLevel, minDivRes, eaThreshold, elThreshold)
}
val compBNGRadigAdaptiveInt_UDF = udf[Array[String], Geometry, Boolean, Int, Int, Double, Double, Double](compBNGRadigAdaptiveInt)
spark.udf.register("compBNGRadigAdaptiveInt_UDF", compBNGRadigAdaptiveInt_UDF)
//
def compBNGRadigAdaptiveIntJSon(geomJSon:String, hasDualDivisor:Boolean, intPolicy:Int, maxDivLevel:Int, minDivRes:Double, eaThreshold:Double, elThreshold:Double):Array[String]={
  val geom:Geometry = geojson2Geom(geomJSon)  
  Radig_BNG.compBNGRadigAdaptiveInt(geom, hasDualDivisor, intPolicy, maxDivLevel, minDivRes, eaThreshold, elThreshold)
}
val compBNGRadigAdaptiveIntJSon_UDF = udf[Array[String], String,  Boolean, Int, Int, Double, Double, Double](compBNGRadigAdaptiveIntJSon)
spark.udf.register("compBNGRadigAdaptiveIntJSon_UDF", compBNGRadigAdaptiveIntJSon_UDF)
//
def compBNGRadigAdaptiveCnt(geom:Geometry, hasDualDivisor:Boolean, intPolicy:Int, maxDivLevel:Int, minDivRes:Double, eaThreshold:Double, elThreshold:Double):Array[Array[String]]={
  Radig_BNG.compBNGRadigAdaptiveCnt(geom, hasDualDivisor, intPolicy, maxDivLevel, minDivRes, eaThreshold, elThreshold)
}
val compBNGRadigAdaptive_UDF = udf[Array[Array[String]], Geometry, Boolean, Int, Int, Double, Double, Double](compBNGRadigAdaptiveCnt)
spark.udf.register("compBNGRadigAdaptiveIntJSon_UDF", compBNGRadigAdaptiveIntJSon_UDF)
//
def compBNGRadigAdaptiveCntJSon(geomJSon:String, hasDualDivisor:Boolean, intPolicy:Int, maxDivLevel:Int, minDivRes:Double, eaThreshold:Double, elThreshold:Double):Array[Array[String]]={
  val geom:Geometry = geojson2Geom(geomJSon)  
  Radig_BNG.compBNGRadigAdaptiveCnt(geom, hasDualDivisor, intPolicy, maxDivLevel, minDivRes, eaThreshold, elThreshold)
}

// COMMAND ----------

//val str = "{\"type\":\"Polygon\",\"coordinates\":[[[458709.44,242642.85],[458707.4,242656.99],[458706.3,242663.49],[458705.3,242669.96],[458703.5,242681.8],[458702.79,242686.6],[458702.1,242691.6],[458701.2,242697.2],[458700.4,242702.8],[458699.7,242707.39],[458698.9,242712.09],[458697.3,242723.59],[458696.39,242729.2],[458695.69,242734.8],[458694.5,242742.19],[458693.19,242749.62],[458692.1,242755.89],[458691.1,242762.18],[458689.8,242768.56],[458688.4,242774.77],[458687.0,242782.17],[458685.5,242789.67],[458684.0,242796.77],[458682.6,242803.77],[458681.2,242810.25],[458679.8,242816.67],[458678.7,242822.78],[458677.6,242828.77],[458674.2,242844.36],[458672.7,242850.66],[458671.2,242857.07],[458669.9,242863.67],[458668.4,242870.37],[458667.2,242875.86],[458665.91,242881.35],[458664.51,242886.74],[458663.01,242892.24],[458661.91,242896.54],[458660.61,242900.82],[458658.91,242906.32],[458657.32,242911.71],[458654.31,242921.73],[458652.11,242928.82],[458649.91,242936.02],[458647.81,242942.52],[458645.82,242949.01],[458643.31,242957.12],[458640.91,242965.32],[458635.61,242983.03],[458630.36,243000.0],[458627.926,243007.903],[458624.683,243006.824],[458622.027,243006.705],[458618.442,243007.965],[458613.65,243009.65],[458616.56,243000.0],[458617.84,242990.59],[458617.91,242989.74],[458618.86,242979.6],[458620.0,242969.48],[458626.92,242928.0],[458637.8,242865.62],[458652.79,242776.23],[458665.06,242701.97],[458672.38,242661.29],[458675.45,242644.55],[458709.44,242642.85]]]}"
//val geom = geojson2Wkt(str)

// COMMAND ----------

// substringing a single string, returning an array
def RadigRefToSubStrings(str:String, hdLen:Int, segLen:Int, minLen:Int, maxLen:Int):Array[String]={
  val len:Int = str.length
  var numSeg = (len - hdLen) / segLen
  if(maxLen > 0 && maxLen < len){
    numSeg = (maxLen - hdLen) / segLen
  }
  var firstSeg = 0
  if(minLen > hdLen)
    firstSeg = (minLen-hdLen)/segLen
  val rlts:Array[String] = new Array[String](numSeg-firstSeg+1)
  for(i <- firstSeg to numSeg){
    val end:Int = hdLen+segLen*i
    rlts(i-firstSeg)= str.substring(0, end)
  }
  rlts
}
//
// substringing a single string, returning via a Set (to remove duplicates from other reference string in an input string array)
// minLen and maxLen (inclusive) are length restrictions on returned substrings. Length includes the length of the header. If 0 or negative value is supplied, they will be ignored
//
def RadigRefToSubStrings(str:String, hdLen:Int, segLen:Int, minLen:Int, maxLen:Int, strSet:collection.mutable.Set[String])={
  val len:Int = str.length
  var numSeg = (len - hdLen) / segLen
  if(maxLen > 0 && maxLen < len){
    numSeg = (maxLen - hdLen) / segLen
  }
  var firstSeg = 0
  if(minLen > hdLen)
    firstSeg = (minLen-hdLen)/segLen
  for(i <- firstSeg to numSeg){
    val end:Int = hdLen+segLen*i
    strSet+= str.substring(0, end)
  }
}
//
// substringing an array of strings, returning an array of strings
//
def RadigRefSetToSubStrings(strs:Array[String], hdLen:Int, segLen:Int):Array[String]={
  var strSet:collection.mutable.Set[String] = collection.mutable.Set.empty[String]
  for(str<-strs){
    RadigRefToSubStrings(str, hdLen, segLen, 0, 0, strSet)
  }
  strSet.toArray
}
val RadigRefSetToSubStrings_UDF = udf[Array[String], Array[String], Int, Int](RadigRefSetToSubStrings)
spark.udf.register("RadigRefSetToSubStrings_UDF", RadigRefSetToSubStrings_UDF)
//
// substringing an array of strings, with minimum length restriction
//
def RadigRefSetToSubStrings(strs:Array[String], hdLen:Int, segLen:Int, minLen:Int):Array[String]={
  var strSet:collection.mutable.Set[String] = collection.mutable.Set.empty[String]
  for(str<-strs){
    RadigRefToSubStrings(str, hdLen, segLen, minLen, 0, strSet)
  }
  strSet.toArray
}
val RadigRefSetToSubStringsMin_UDF = udf[Array[String], Array[String], Int, Int, Int](RadigRefSetToSubStrings)
spark.udf.register("RadigRefSetToSubStringsMin_UDF", RadigRefSetToSubStringsMin_UDF)
//
// substringing an array of strings, with minimum and maximum length restrictions
//
def RadigRefSetToSubStrings(strs:Array[String], hdLen:Int, segLen:Int, minLen:Int, maxLen:Int):Array[String]={
  var strSet:collection.mutable.Set[String] = collection.mutable.Set.empty[String]
  for(str<-strs){
    RadigRefToSubStrings(str, hdLen, segLen, minLen, maxLen, strSet)
  }
  strSet.toArray
}
val RadigRefSetToSubStringsMinMax_UDF = udf[Array[String], Array[String], Int, Int, Int](RadigRefSetToSubStrings)
spark.udf.register("RadigRefSetToSubStringsMinMax_UDF", RadigRefSetToSubStringsMinMax_UDF)
//

// COMMAND ----------

// RadigRefToSubStrings("abc12345678", 3, 2, 0, 0)

// COMMAND ----------

// RadigRefSetToSubStrings(Array("abc12345678", "abc87654321"), 3, 2, 7, 9)

// COMMAND ----------

// val geom = wkt2Geom("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
// val refs = compBNGRadigResInt(geom, true, 1000.0, true, 1)