import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  var centroids: Array[Point] = Array[Point]()
  var given_points : Array[Point] = Array[Point]()
  var c : Array[(Point, Point)] = Array[(Point, Point)]()
  type Point = (Double, Double); 
  var i:Int=0;
  def find_new_centroid(cent : Point , points : Seq[Point]) : Point = 
  {
  var count : Int = 0;
  var sx : Double = 0.00;
  var sy : Double = 0.00;
  i=0
  while( i < points.length )
  {
    sx=points(i)._1
    sy=points(i)._2
    i=i+1
  }
  val centx=sx/i
  val centy=sy/i
  return (centx,centy);
  }

  def centroid_dist_cal(cent : Array[Point], point : Point) : Point = 
  {
  
    var min_dist : Double = 10000;
    var dist : Double = 10000;
    var min_index : Int = 10000;
    var count : Int = 0;
    i=0
    while(i < cent.length)
    {
      val x_axis:Double =((cent(i)._1-point._1)*(cent(i)._1 - point._1))
      val y_axis:Double =((cent(i)._2-point._2)*(cent(i)._2-point._2))
      dist = Math.sqrt((x_axis) + (y_axis));
      if(dist < min_dist)
      {
        min_dist = dist;
        min_index = count;
      }
      count = count + 1
      i=i+1
    }
    
    return (cent(min_index)._1, cent(min_index)._2)
  }


  def main(args: Array[ String ]) 
  {
    val conf = new SparkConf().setAppName("KMeans")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    centroids = sc.textFile(args(0)).collect.map(line =>{ val a = line.split(",") 
                                                        (a(0).toDouble,a(1).toDouble)})
    given_points = sc.textFile(args(1)).collect.map(line =>{ val a = line.split(",") 
                                                        (a(0).toDouble, a(1).toDouble)}) 
    sc.stop()
    for ( i <- 1 to 5 )
    {
      c = given_points.map(d => {((centroid_dist_cal(centroids, d), (d._1, d._2)))})
      for(i <- 0 until given_points.length)
      centroids :+ find_new_centroid(given_points(i),given_points);
    }
    centroids.foreach(println)
    //centroids.saveAsTextFile(args(2))
  }
}


