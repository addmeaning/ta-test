import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.matching.Regex

object TAtest extends App {

  import org.apache.spark.sql.SparkSession

  private implicit val sparkSession: SparkSession = SparkSession.builder().appName("ta-test").master("local[*]").getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  private val basePath = "src\\main\\resources\\economics.stackexchange.com\\"
  private val usersPath = basePath + "Users.xml"
  private val postsPath = basePath + "Posts.xml"


  Answers.firstQuestion(usersPath)
  Answers.secondQuestion(postsPath)
}


object Answers {

  private def transformToUserCaseClass(str: String): Iterator[UserLine] = {
    val pattern = """.*(Location)=\"(.*?)\".*(UpVotes)=\"(.*?)\".*(DownVotes)=\"(.*?)\".*""".r
    transformToCaseClass(str, pattern, m => UserLine(m.group(2), m.group(4), m.group(6)))
  }

  private def transformToPostCaseClass(str: String): Iterator[PostLine] = {
    val pattern = """.*(Tags)=\"(.*?)\".*""".r
    transformToCaseClass(str, pattern, m => PostLine(m.group(2)))
  }

  private def transformToCaseClass[T](string: String, pattern: Regex, function: Regex.Match => T): Iterator[T] = {
    if (!string.startsWith("  <row ")) Iterator.empty
    else pattern.findAllMatchIn(string).map(function)
  }

  /** this method answers first question:
    *
    * @param usersPath dataframe of users
    * @param session   underlying session for implicit conversions
    */
  def firstQuestion(usersPath: String)(implicit session: SparkSession): Unit = {
    import session.implicits._
    val users: DataFrame = session.sparkContext.textFile(usersPath)
      .flatMap(str => transformToUserCaseClass(str))
      .toDF("location", "upvotes", "downvotes")
      .groupBy("location")
      .agg(sum('upvotes).as("total_upvotes"), sum('downvotes).as("total_downvotes"))

    users.cache()
    users.orderBy('total_upvotes.desc).select('location).show(1) //z tej lokacji użytkownicy dostali najwięcej upvote'ów
    users.orderBy('total_downvotes.desc).select('location).show(1) //z tej lokacji użytkownicy dostali najwięcej downvote'ów
    users.unpersist()
  }

  /** this method answers second question
    *
    * @param postsPath path to posts dataframe
    * @param session  underlying session for implicit conversions
    */
  def secondQuestion(postsPath: String)(implicit session: SparkSession): Unit = {
    import session.implicits._
    val tags: DataFrame = session.sparkContext.textFile(postsPath).flatMap(str => transformToPostCaseClass(str)).toDF("tags")
    tags.select(explode('tags).alias("tags")).groupBy('tags).agg(count("*").alias("count")).orderBy('count.desc).show(10)
  }
}

object PostLine{
  def apply(tags: String): PostLine = new PostLine("""&lt;(.*?)&gt;""".r.findAllMatchIn(tags).map(_.group(1)).toList)
}
case class PostLine(tags: List[String])

object UserLine {
  def apply(location: String, upMod: String, downMod: String): UserLine = new UserLine(processLocation(location), upMod.toInt, downMod.toInt)

  /**
    * Dummy function for string processing. This is neither performant nor complete.
    * For example USA and United States of America considered different, as well as NY and New Your.
    * I believed this wasn't exercise about text analysis, so I applied only obvious transformations
    *
    * @param str input string
    * @return string without non-standard characters & without
    */
  private def processLocation(str: String): String =
    str.toUpperCase
      .replace(",", "")
      .replaceAll("Â|À|Å|Ã", "A")
      .replaceAll("Â|À|Å|Ã", "A")
      .replaceAll("Ä", "AE")
      .replaceAll("Ç", "C")
      .replaceAll("Í", "I")
      .replaceAll("É|Ê|È|Ë", "E")
      .replaceAll("Ó|Ô|Ò|Õ|Ø", "O")
      .replaceAll("Ö", "OE")
      .replaceAll("Š", "S")
      .replaceAll("Ú|Û|Ù", "U")
      .replaceAll("Ü", "UE")
      .replaceAll("Ý|Ÿ", "Y")
      .replaceAll("Ž", "Z")
}

case class UserLine(location: String, upMod: Int, downMod: Int)

