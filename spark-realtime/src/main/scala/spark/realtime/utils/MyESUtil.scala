package spark.realtime.utils

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Get, Index, Search, SearchResult}
import java.util

import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilder, QueryShardContext, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

object MyESUtil {

  private var jestFactory: JestClientFactory = null

  def getJestClient(): JestClient = {
    if (jestFactory == null) {
      //创建 Jest 客户端工厂对象
      build()
    }
    jestFactory.getObject
  }

  def build(): Unit = {
    jestFactory = new JestClientFactory
    jestFactory.setHttpClientConfig(new HttpClientConfig
    .Builder("http://cdh-01:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(5000)
      .build()
    )
  }

  // 向 es 中插入单条数据 方法一 json形式直接传递
  def putIndex1(): Unit = {
    val jestClient: JestClient = getJestClient()

    // 定义执行的 source
    var source: String =
      """
        |
        |""".stripMargin
    //创建插入类 index
    val index: Index = new Index.Builder(source)
      .index("movie_index_1")
      .`type`("movie")
      .id("1").build()

    //通过客户端对象操作 es
    jestClient.execute(index)

    //关闭连接
    jestClient.close()
  }

  // 向 es 中插数据 方式二 将插入文档封装成样例类对象
  def putIndex2(): Unit = {
    val jestClient = getJestClient()

    var actorList: util.ArrayList[util.Map[String, Object]] = new util.ArrayList[util.Map[String, Object]]()
    var actorMap1 = new util.HashMap[String, Object]()
    actorMap1.put("id", "66")
    actorMap1.put("name", "wangyuyan")
    actorList.add(actorMap1)

    // 封装样例类对象
    Movie(300, "tianlongbabu", 9.0f, actorList)
    // 创建 Action 实现类  ===> Index
    val index = new Index.Builder()
      .index("movie_index_2")
      .`type`("movie")
      .id("2")
      .build()
    jestClient.execute(index)
    jestClient.close()
  }

  // 根据文档的id， 从 es 中查询出一条数据
  def queryIndexById(): Unit = {
    val jestClient = getJestClient()

    val get = new Get.Builder("movie_index_1", "2").build()

    val result = jestClient.execute(get)
    println(result.getJsonString)

    jestClient.close()
  }

  // 根据指定查询条件，从 es 中查询多个文档 方法一
  def queryIndexByCondition1(): Unit = {
    val jestClient = getJestClient()

    var query: String =
      """
        |
        |""".stripMargin
    val search = new Search.Builder(query)
      .addIndex("move_index_1")
      .build()
    val result = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import scala.collection.JavaConverters._
    val resultList: List[util.Map[String, Any]] = list.asScala.map(_.source).toList

    println(resultList.mkString("\n"))

    jestClient.close()
  }

  def queryIndexByCondition2(): Unit = {
    val jestClient = getJestClient()

    //SearchSourceBuilder 用于构建查询 json 格式字符串
    val sourceBuilder = new SearchSourceBuilder()
    val boolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name", "tianlong"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword", ""))

    sourceBuilder.query(boolQueryBuilder)
    sourceBuilder.from(0)
    sourceBuilder.size(10)
    sourceBuilder.sort("doubanScore", SortOrder.ASC)
    sourceBuilder.highlighter(new HighlightBuilder().field("name"))

    val query: String = sourceBuilder.toString

    val search = new Search.Builder(query)
      .addIndex("move_index_1")
      .build()
    val result = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import scala.collection.JavaConverters._
    val resultList: List[util.Map[String, Any]] = list.asScala.map(_.source).toList

    println(resultList.mkString("\n"))

    jestClient.close()
  }

  def main(args: Array[String]): Unit = {
    putIndex1()
  }
}


case class Movie(id: Long, name: String, doubanScore: Float, actorList: util.List[util.Map[String, Object]]) {}

