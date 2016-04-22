package algorithms.connectedcomponents

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.scalatest.FunSuite

/**
  * Created by galpha on 4/15/16.
  */
class ConnectedComponentsTest extends FunSuite with SharedSparkContext {
  test("ConnectedComponents.scala CC") {
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )


    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
    val graph = Graph(vertexRDD, edgeRDD)

    val ccGraph = ConnectedComponents.run(graph = graph, 3)

    ccGraph.vertices.collect().foreach(println)

  }
}
