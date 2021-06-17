  
package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    if (queryRectangle == null || pointString == null)
        return false

    val Rctngle_Coord = queryRectangle.split(",").map(_.toDouble)
    var X1 = Rctngle_Coord(0)
    var Y1 = Rctngle_Coord(1)
    var X2 = Rctngle_Coord(2)
    var Y2 = Rctngle_Coord(3)
    
    val Pnt_Coord = pointString.split(",").map(_.toDouble)
    val PX = Pnt_Coord(0)
    val PY = Pnt_Coord(1)
    
    if ((Math.min(X1,X2)<=PX) && (Math.max(X1,X2)>=PX) && (Math.min(Y1,Y2)<=PY) && (Math.max(Y1,Y2)>=PY)){
      return true
    }
    else { return false }
  }
}

