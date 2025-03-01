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
 /* Utility class to generate regular grid cells
 *  the grid cells will be in the rectangular region of (minX minY, maxX maxY)
 *  cell width and height may be different
 *  cell id represents the 2D array index of the cell
 *
 */
package uk.osgb.algorithm.or4nn;

import org.locationtech.jts.geom.*;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


/**
 *  regular grid generator
 */
public class GridGenerator {
    /**
     *  Name for the grid
     */
    String gridName;
    /**
     *  Extent of the grid
     */
    double minX, minY, maxX, maxY;
    /**
     *  cell width on X/Y
     */
    double xWidth, yWidth;
    /**
     * number of cells on X/Y dimension
     */
    int xDim, yDim;
    /**
     * grid cell id index length on X/Y (shorter id will be padded with 0 to the length)
     */
    int xIdxLen, yIdxLen;
    /**
     *  grid cell array
     */
    GridCell[][] cells = null;
    public GridGenerator(String gridName, double minX, double minY, double maxX, double maxY, double xWidth, double yWidth) {
        this.gridName = gridName;
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
        this.xWidth = xWidth;
        this.yWidth = yWidth;
        double xExt = maxX - minX;
        double yExt = maxY - minY;
        xDim = (int) Math.round(xExt / xWidth);
        yDim = (int) Math.round(yExt / yWidth);
        xIdxLen = Integer.toString(xDim-1).length();
        yIdxLen = Integer.toString(yDim-1).length();
    }
    String generateCellID(int xIdx, int yIdx){
        // check if in range
        if(xIdx < 0 || yIdx < 0 || xIdx >= xDim || yIdx >= yDim){
            return null;
        }
        String xStr = String.format("%0"+xIdxLen+"d", xIdx);
        String yStr = String.format("%0"+yIdxLen+"d", yIdx);
        return xStr+yStr;
    }
    /**
     *
     * @param cell_Id
     * @param nbIdx adjacent cell index, 0 for E, 1 for NE, 2 for N, ..., 7 for SE
     * @return the string cell id of the adjacent cell, or null if the cell is out of boundary
     */
    public String getAdjCellID(String cell_Id, int nbIdx){
        String xStr = cell_Id.substring(0, xIdxLen);
        String yStr = cell_Id.substring(xIdxLen, xIdxLen + yIdxLen);
        int x = Integer.parseInt(xStr);
        int y = Integer.parseInt(yStr);
        switch(nbIdx){
            case 0: x+=1;
                    break;
            case 1: x+=1;
                    y+=1;
                    break;
            case 2: y+=1;
                    break;
            case 3: x-=1;
                    y+=1;
                    break;
            case 4: x-=1;
                    break;
            case 5: x-=1;
                    y-=1;
                    break;
            case 6: y-=1;
                    break;
            case 7: x+=1;
                    y-=1;
                    break;
            default: return null; // invalid nb index
        }
        return generateCellID(x, y);
    }
    /**
     *
     */
    public String[] getNBCellIDs(String cell_Id, int ringIdx){
        String xStr = cell_Id.substring(0, xIdxLen);
        String yStr = cell_Id.substring(xIdxLen, xIdxLen + yIdxLen);
        int x = Integer.parseInt(xStr);
        int y = Integer.parseInt(yStr);
        int xL = x - ringIdx;
        int xR = x + ringIdx;
        int yB = y - ringIdx;
        int yT = y + ringIdx;
        List<String> ids = new ArrayList<>();
        // bottom and top rows
        for(int i = xL; i <= xR; ++i){
            String idB = generateCellID(i, yB);
            String idT = generateCellID(i, yT);
            if(idB!=null) {
                ids.add(idB);
            }
            if(idT!=null) {
                ids.add(idT);
            }
        }
        //left and right columns
        for(int j = yB+1; j < yT; ++j){
            String idL = generateCellID(xL, j);
            String idR = generateCellID(xR, j);
            if(idL!=null) {
                ids.add(idL);
            }
            if(idR!=null) {
                ids.add(idR);
            }
        }
        String[] idArray = new String[ids.size()];
        ids.toArray(idArray);
        return idArray;
    }
    public void generateGrid(){
        GeometryFactory gf = new GeometryFactory();
        cells = new GridCell[xDim][yDim];
        for(int i = 0; i < xDim; ++i){
            for(int j = 0; j < yDim; ++j){
                String cell_id = generateCellID(i, j);
                cells[i][j] = new GridCell(cell_id, minX+xWidth*i, minY+yWidth*j, xWidth, yWidth, gf);
            }
        }
    }
    public List<GridCell> getCellList(){
        if(cells == null)
            return null;
        List<GridCell> cellList = new ArrayList<>();
        for(int i = 0; i < xDim; ++i){
            for(int j = 0; j < yDim; ++j){
                cellList.add(cells[i][j]);
            }
        }
        return  cellList;
    }
    public void exportToCSV(String path, char delim){
        PrintWriter writer = null;
        String header = "cell_id"+delim+"cell_bnd";
        if(cells != null) {
            try {
                writer = new PrintWriter(path);
                writer.println(header);
                for (int i = 0; i < xDim; ++i) {
                    for (int j = 0; j < yDim; ++j) {
                        writer.println(cells[i][j].toText(delim));
                    }
                }
                writer.close();
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally {
                try {
                    writer.close();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
    // predefined grids, for meta data and adjacent cell search
    public static GridGenerator GB1KM = new GridGenerator("BNG1km", 0.0, 0.0, 700000.0, 1300000.0, 1000.0, 1000.0);
    public static GridGenerator GB2KM = new GridGenerator("BNG2km", 0.0, 0.0, 700000.0, 1300000.0, 2000.0, 2000.0);
    public static GridGenerator GB5KM = new GridGenerator("BNG5km", 0.0, 0.0, 700000.0, 1300000.0, 5000.0, 5000.0);
    public static GridGenerator GB10KM = new GridGenerator("BNG10km", 0.0, 0.0, 700000.0, 1300000.0, 10000.0, 10000.0);

}
