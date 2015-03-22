
import scala.io.Source
import org.apache.spark.graphx._

object PRGraph {
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

		val ranks = graph.pageRank(0.0001)

		val verFile = "hdfs://localhost:9000/usr/local/hadoop-dir/ranksVer.txt";
		val edgeFile = "hdfs://localhost:9000/usr/local/hadoop-dir/ranksEdge.txt";
		
		ranks.vertices.saveAsObjectFile(verFile);
		ranks.edges.saveAsObjectFile(edgeFile);

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
