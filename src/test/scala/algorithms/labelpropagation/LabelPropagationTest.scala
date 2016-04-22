package algorithms.labelpropagation

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.graphx.{Graph, Edge}
import org.scalatest.FunSuite

/**
  * Created by galpha on 4/20/16.
  */
class LabelPropagationTest extends FunSuite with SharedSparkContext {

  test("LabelPropagationTest.scala CC") {

    val vertexArray = Array(
      (0L, ("Alice", 28)),
      (1L, ("Bob", 27)),
      (2L, ("Charlie", 65)),
      (3L, ("David", 42)),
      (4L, ("Ed", 55)),
      (5L, ("Fran", 50)),
      (6L, ("Fran", 50)),
      (7L, ("Fran", 50))
    )
    val edgeArray = Array(
      Edge(0L, 1L, 7),
      Edge(0L, 2L, 2),
      Edge(0L, 3L, 3),
      Edge(1L, 2L, 1),
      Edge(1L, 3L, 8),
      Edge(2L, 3L, 3),
      Edge(2L, 4L, 3),
      Edge(4L, 5L, 3),
      Edge(4L, 6L, 3),
      Edge(4L, 7L, 3),
      Edge(5L, 6L, 3),
      Edge(5L, 7L, 3),
      Edge(6L, 7L, 3)
    )
    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
    val graph = Graph(vertexRDD, edgeRDD)

    val ccGraph = LabelPropagation.run(graph = graph, 3)

    ccGraph.vertices.collect().foreach(println)

  }

}
