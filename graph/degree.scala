
import scala.io.Source
import org.apache.spark.graphx._

object DegreeGraph {
	def main(args:Array[String]) = {
		val filename = args(0);
		val linesIt =  Source.fromFile(filename).getLines();
		val lines = Array[String]("","");
		linesIt.copyToArray(lines);
		val verAddr = lines(0).split("\t")(1);
		val verObj = sc.objectFile[(org.apache.spark.graphx.VertexId, Any)](verAddr);

		val edgeAddr = lines(1).split("\t")(1);
		val edgeObj = sc.objectFile[org.apache.spark.graphx.Edge[Int]](edgeAddr);

		val graph = Graph(verObj, edgeObj);

	
		val outDegrees: VertexRDD[Int] = graph.outDegrees
		val degreeGraph = graph.outerJoinVertices(outDegrees) { (id, oldAttr, outDegOpt) =>
  			outDegOpt match {
    			case Some(outDeg) => outDeg
    			case None => 0 // No outDegree means zero outDegree
 		 		}
		}

		val verFile = "hdfs://localhost:9000/usr/local/hadoop-dir/degreeVer.txt";
		val edgeFile = "hdfs://localhost:9000/usr/local/hadoop-dir/degreeEdge.txt";
		
		degreeGraph.vertices.saveAsObjectFile(verFile);
		degreeGraph.edges.saveAsObjectFile(edgeFile);

		val pw = new PrintWriter(new File(args(1)));

		pw.write("graphVer");
		pw.write("\t");
		pw.write(verFile);
		pw.write("\n");


		pw.write("graphEdge");
		pw.write("\t");
		pw.write(edgeFile);
		pw.write("\n");
		pw.close
				
	}
}
