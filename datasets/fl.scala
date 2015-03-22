
import scala.io.Source
import java.io._
import org.apache.spark.graphx._

object Flickr {
	def main(args:Array[String]) = {
		val filename = "hdfs://localhost:9000/usr/local/hadoop-dir/fl.txt";
		val inputGraph = GraphLoader.edgeListFile(sc, filename);
	
		val verFile = "hdfs://localhost:9000/usr/local/hadoop-dir/flickrVer.txt";
		val edgeFile = "hdfs://localhost:9000/usr/local/hadoop-dir/flickrEdge.txt";
		
		inputGraph.vertices.saveAsObjectFile(verFile);
		inputGraph.edges.saveAsObjectFile(edgeFile);

		
		val pw = new PrintWriter(new File(args(0)));

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
