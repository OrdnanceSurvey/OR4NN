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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class GridCell {
    Geometry bnd;
    Envelope rangeRect = null;
    Geometry objRange = null;
    String cell_id;
    //
    public GridCell(String id, double baseX, double baseY, double widthX, double widthY, GeometryFactory gf){
        cell_id = id;
        Envelope rect = new Envelope(baseX, baseX+widthX, baseY, baseY + widthY);
        bnd = gf.toGeometry(rect);
    }

    /** no point to use this anymore
     *
     * @param offsetX
     * @param offsetY
     */
    @Deprecated
    public void generateDefaultObjectRange(double offsetX, double offsetY){
        Envelope rect = bnd.getEnvelopeInternal();
        rect.expandBy(offsetX, offsetY);
        objRange = bnd.getFactory().toGeometry(rect);
        //objRange = GridGenerator.rect2Polygon(rect, bnd.getFactory());
    }
    //
    String toText(char delim){
        String txt = cell_id + delim + (bnd!=null?bnd.toText():"");
        return txt;
    }
    String[] toTextArray(){
        String[] rtn = new String[2];
        try {
            rtn[0] = cell_id;
            rtn[1] = bnd.toText();
        }catch (NullPointerException e){
            e.printStackTrace();
        }
        return rtn;
    }
}
