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
package uk.osgb.algorithm.or4nn;

import org.locationtech.jts.algorithm.distance.DistanceToPoint;
import org.locationtech.jts.algorithm.distance.PointPairDistance;
import org.locationtech.jts.geom.*;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import uk.osgb.datastructures.MultiTreeMap;
public class ObjectRangeComputer {
    /** Compute object range for region qrwExt, where object set are points
     *
     * @param qrwExt
     * @param pointObjs
     * @param maxK
     * @param numCircumgonEdges
     * @return Object range polygon
     */
    public static Geometry compPointObjRange(Geometry qrwExt, Geometry[] pointObjs, int maxK, int numCircumgonEdges){
        Geometry ch = qrwExt.convexHull();
        return null;
    }

    /** Compute rectanglar object range for region qrwExt, where object set are points
     *
     * @param qrwExt
     * @param pointObjs
     * @param maxK
     * @return Object range rectangle as a Geometry
     */
    public static Geometry compObjRangeRect(Geometry qrwExt, Geometry[] pointObjs, int maxK){
        Geometry ch = qrwExt.convexHull();
        Coordinate[] coords = ch.getCoordinates();
        Geometry[] filteredObjs = filterObjects(qrwExt, pointObjs, maxK);
        Envelope range = null;
        for(Coordinate coord:coords){
            double fDist = furthestDistanceToObjects(coord, filteredObjs, null);
            Envelope env = objRangeForPointRect(coord, fDist);
            if(range!= null){
                range.expandToInclude(env);
            }else{
                range = env;
            }
        }
        if(range == null){
            System.out.println("Error: range not extended...");
        }
        return qrwExt.getFactory().toGeometry(range);
    }

    /** Compute object range at region qrwExt, where object set are polygons
     *
     * @param qrwExt
     * @param polygonObjs
     * @param numCircumgonEdges
     * @return
     */
    public static Geometry compObjRangePolygon(Geometry qrwExt, Geometry[] polygonObjs, int numCircumgonEdges){
        Geometry ch = qrwExt.convexHull();

        return null;
    }

    /** Compute rectanglar object range for region qrwExt, where object set are polygons
     *
     * @param qrwExt
     * @param polygonObjs
     * @return
     */
    public static Geometry compObjRangePolygonRect(Geometry qrwExt, Geometry[] polygonObjs, int maxK){
        Geometry ch = qrwExt.convexHull();

        return null;
    }

    /** find the most "central" maxK objects from the input oject set
     *
     * @param qrwExt
     * @param objects
     * @param maxK
     * @return
     */
    static Geometry[] filterObjects(Geometry qrwExt, Geometry[] objects, int maxK){
        if(objects.length <= maxK){
            return objects;
        }
        Geometry[] rlts = new Geometry[maxK];
        Point cen = qrwExt.getCentroid();
        // need to use multimap
        MultiTreeMap<Double, Geometry> distIdx = new MultiTreeMap<Double, Geometry>();
        for(Geometry geom:objects){
            distIdx.put(cen.distance(geom), geom);
        }
        for(int i = 0; ;){
            Collection<Geometry> objs = distIdx.pollFirstValue();
            for(Geometry obj:objs){
                rlts[i] = obj;
                i++;
                if(i== maxK){
                    return rlts;
                }
            }
        }
    }
    /** compute the furthest distance from a location to a set of geometries
     *
     * @param coord
     * @param objs
     * @param vec if not null, the vector from coord to the furthest vertex will be returned via it
     * @return the furthest distance
     */
    public static double furthestDistanceToObjects(Coordinate coord, Geometry[] objs, Coordinate vec){
        double furDist = -1.0; // negative value so zero distance is accounted
        Coordinate tgt = new Coordinate();
        for(Geometry obj:objs){
            double dist = furthestDistanceToObject(coord, obj, tgt);
            if(dist > furDist){
                furDist = dist;
                if(vec!=null)
                    vec.setCoordinate(tgt);
            }
        }
        return furDist;
    }
    static double furthestDistanceToObject(Coordinate base, Geometry obj, Coordinate vec){
// computation of ch may take more time than simply compute distance to all vertices
//        Geometry ch = obj.convexHull();
//        Coordinate[] coords = ch.getCoordinates();
        Coordinate[] coords = obj.getCoordinates();
        double furDist = -1.0;
        Coordinate tgt = null;
        for(Coordinate coord:coords){
            double dist = base.distance(coord);
            if(dist > furDist){
                furDist = dist;
                tgt = coord;
            }
        }
        if(vec!=null){
            vec.x = tgt.x - base.x;
            vec.y = tgt.y - base.y;
        }
        return furDist;
    }

    static double furthestDistanceToObject(Point point, Geometry obj, Coordinate vec){
// computation of ch may take more time than simply compute distance to all vertices
//        Geometry ch = obj.convexHull();
//        Coordinate[] coords = ch.getCoordinates();
        Coordinate[] coords = obj.getCoordinates();
        double furDist = -1.0;
        Coordinate base = point.getCoordinate();
        Coordinate tgt = null;
        for(Coordinate coord:coords){
            double dist = base.distance(coord);
            if(dist > furDist){
                furDist = dist;
                tgt = coord;
            }
        }
        if(vec!=null){
            vec.x = tgt.x - base.x;
            vec.y = tgt.y - base.y;
        }
        return furDist;
    }
    /**
     *
     * @param coord
     * @param objs
     * @return
     */
    static PointPairDistance compVectorToFurthestObj(Coordinate coord, Geometry[] objs){
        double furDist = -1.0;
        PointPairDistance ppDist = null;
        for(Geometry obj:objs){
            PointPairDistance ptDist = new PointPairDistance();
            DistanceToPoint.computeDistance(obj, coord, ptDist);
            double curDist = ptDist.getDistance();
            if(ppDist == null || curDist > furDist){
                furDist = curDist;
                ppDist = ptDist;
            }
        }
        return ppDist;
    }

    /**
     *
     * @param base
     * @param tgt
     * @param radius
     * @param tangentNum
     * @return
     */
    static Geometry objRangeForPoint(Coordinate base, Coordinate tgt, double radius, int tangentNum){
        return null;
    }

    /** Compute the rectanglar object range
     *
     * @param base
     * @param radius
     * @return
     */
    public static Envelope objRangeForPointRect(Coordinate base, double radius){
        Envelope env = new Envelope(base.x-radius, base.x + radius, base.y - radius, base.y + radius);
        return env;
    }
}
