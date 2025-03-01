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

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.ItemDistance;

public class KNNItemDistance implements ItemDistance {
    /**
     * @param itemBoundable
     * @param itemBoundable1
     * @return
     */
    @Override
    public double distance(ItemBoundable item1, ItemBoundable item2) {
        if(item1 == item2)
            return Double.MAX_VALUE;
        Geometry geom1 = (Geometry) item1.getItem();
        Geometry geom2 = (Geometry) item2.getItem();
        return geom1.distance(geom2);
    }
}
