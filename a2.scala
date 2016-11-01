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

        val smallRowLength = 504
        val smallColLength = 24

        val ofile = new File(outfile)
        val output = new BufferedWriter(new FileWriter(ofile))

        //distributed matrix factorization

        val trainSet = readInDatePoints(datafile)

        val R = Array.fill[Array[Double]](smallRowLength)(Array.fill[Double](smallColLength)(0.0))

        trainSet.map(t => {
            R(t._1)(t._2) = t._3
        })


        //output.write(x._1+","+x._2+","+x._3+"\n") //need to write out values to missing coordinates

        output.close()
        System.exit(0)
    }
}
