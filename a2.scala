import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._

object Assign2 {

    def readInDatePoints(datafile: String): Iterator[(Int, Int, Double)] = {
        val lines =  Source.fromFile(datafile).getLines()
                        .map(_.split(","))
                        .map(x =>(x(0).toInt, x(1).toInt, x(2).toDouble))
        return lines
    }

    def main(args: Array[String])
    {
        val datafile = args(0)
        val missingfile = args(1)
        val outfile = args(2)

        val rowLength = 504
        val colLength = 24

        if(datafile == "small.csv") {
          val rowLength = 504
          val colLength = 24
        } else if(datafile == "medium.csv") {
          val rowLength = 52578
          val colLength = 159
        } else if(datafile == "large.csv") {
          val rowLength = 18988
          val colLength = 1036
        } else {
          val rowLength = 504
          val colLength = 24
        }

        val ofile = new File(outfile)
        val output = new BufferedWriter(new FileWriter(ofile))

        var conf = new SparkConf().setAppName("Assign2")
        var sc = new SparkContext(conf)

        //distributed matrix factorization

        val trainSet = readInDatePoints(datafile)

        var R = Array.fill[Array[Double]](rowLength)(Array.fill[Double](colLength)(0.0))

        trainSet.foreach( t => {
          R(t._1)(t._2) = t._3
        })

        val rows = sc.parallelize(R.map(row => Vectors.dense(row)))

        val rowMatrix = new RowMatrix(rows)

        val svd = rowMatrix.computeSVD(5, true)

        val U = svd.U
        val s = Matrices.diag(svd.s)
        val V = svd.V

        val A = U.multiply(s).multiply(V.transpose).rows.collect.zipWithIndex

        A.foreach(t => {
          var row = t._2
          var vec = t._1
          for(i <- 0 until colLength) {
            output.write(row+","+i+","+vec(i)+"\n")
          }
        })

        //output.write(x._1+","+x._2+","+x._3+"\n") //need to write out values to missing coordinates

        output.close()
        System.exit(0)
    }
}
