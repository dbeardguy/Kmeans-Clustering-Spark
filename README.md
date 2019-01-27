# Kmeans-Clustering-Spark
The following code implements the Kmeans clustering through spark and scala.
The Psuedo-code:
centroids = /* initial centroids from the file centroids.txt */

    for ( i <- 1 to 5 ) {
       val cs = sc.broadcast(centroids)
       centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
                         .groupByKey().map { /* ... calculate a new centroid ... */ }

    }
where distance(x,y) calculates the distance between two points x and y
