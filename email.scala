package email

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset


case class Email(from: String, to: String, subject: String, date: String, realDate: Timestamp)

object TheEmails {
  val dumbEmailAddrRE = new Regex("^.*<([^>]+)>.*$")

  def normalizeEmail(in: String): Option[Timestamp] = {
    if (in == null) {
      return None
    }

    val dumbEmailDateFormat = new SimpleDateFormat("dd MMM yyyy HH:mm:ss Z") // 13 Aug 2021 09:18:14 -0700
    val str = in
      .replaceAll("\"?(GM|U)T\"?$", "+0000") // replace UT, GMT, and the rare "GMT" with +0000
      .replaceAll(" \\([A-Z]+\\)$", "")      // strip " (UTC)" suffix
      .replaceAll("^.*?, ", "")              // remove "Wed, " prefix

    try {
      return Some(new Timestamp (dumbEmailDateFormat.parse(str).getTime()))
    } catch {
      case ex:Throwable => {
        println("couldn't parse date because: '" + ex + "'; was trying to parse '" + str + "'")
        return None
      }
    }
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Emails")
      .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
      .config("spark.executor.instances", "1")
      .config("spark.executor.cores", "1")
      .getOrCreate()


    import spark.implicits._
    if (args.length < 1) {
      print("Usage: Emails <email_file_dataset>")
      sys.exit(1)
    }

    val f = args(0)
    var df = spark.read
      .schema("From STRING, To STRING, Subject STRING, Date STRING, RealDate TIMESTAMP")
      .format("json")
      .load(f)

    var ds = df.as[Email].flatMap({ e =>
      val from = dumbEmailAddrRE.replaceAllIn(e.from, { m:Match => m.group(1) })

      val to = if(e.to != null) {
        dumbEmailAddrRE.replaceAllIn(e.to, { m:Match => m.group(1) })
      } else {
        return Seq()
      }

      val date = normalizeEmail(e.date) match {
        case Some(date) => date
        case None => return Seq()
      }

      Seq(e.copy(from = from, to = to, date = "", realDate = date))
    })

    ds.write.format("parquet").save("eg.parquet")

    println("wrote eg.parquet")
    spark.stop()
  }
}

