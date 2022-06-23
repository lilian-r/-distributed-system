package PageRank

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def run(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PageRank")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val iterateNum = 20
    val factor = 0.85

    val text = sc.textFile(args(0))
    val links = text.map(line => {
      val tokens = line.split(" ")
      var list = List[String]()
      for (i <-2 until tokens.size by 2){
        list = list :+ tokens(i)
      }
      (tokens(0), list)
    }).cache()

    val N = args(0).toLong

    var ranks = text.map(line => {
      val tokens = line.split(" ")
      (tokens(0), tokens(1).toDouble)
    })

    for(iter <- 1 to iterateNum) {
      val contributions = links.join(ranks).flatMap{
        case (page, (links, rank)) =>
          links.map(dest => (dest, rank / links.size))
      }

      ranks = contributions.reduceByKey(_+_)
        .mapValues(v => (1 - factor) * 1.0 / N + factor * v)
    }

    ranks.saveAsTextFile(args(1))
    ranks.foreach(t => println(t._1 + " ", t._2.formatted("%.5f")))
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
   run(args)
  }


//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("Pagerank")//定义一个sparkConf ，	提供Spark运行的各种参数，如程序名称、用户名称等
//    val sc = new SparkContext(conf)//创建Spark的运行环境，并将Spark运行的	参数传入Spark的运行环境中
//    val lines=sc.textFile(args(0));//调用Spark的读文件函数，输出一个RDD类型的实例：lines。具体类型：	RDD[String]
//
//    val links=lines.map{s=>
//      val parts=s.split(" ")
//      (parts(0),parts(1).split(" "))//<原网页，链接网页链表>
//    }.cache()//存在缓存中
//
//    var ranks = links.mapValues(v => 1.0)//初始化网页pr值为1.0
//    //原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素
//
//    for(i <- 0 until 10){//迭代10次
//      val contributions = links.join(ranks).flatMap{//按照key进行内连接
//        case(pageId, (links, rank)) => //对于每个网页获得其对应的pr值和链入表
//          links.map(link => (link, rank / links.size)) //对链入表的每一个网页计算它的贡献值
//      }
//      //flatMap：对集合中每个元素进行操作然后再扁平化。
//
//      //<原网页,(链入网页，贡献pr值)>
//      ranks = contributions
//        .reduceByKey((x,y) => x+y)//将贡献pr值加起来
//        .mapValues(v => (0.15 + 0.85*v))//计算新的pr值
//    }
//    //按照value递减次序排序保留10位小数且输出为一个文件
//    ranks.sortBy(_._2,false).mapValues(v=>v.formatted("%.10f").toString()).coalesce(1,true).saveAsTextFile(args(1));
//
//    /* _._2等价于t => t._2
//	 * map(_._n)表示任意元组tuple对象,后面的数字n表示取第几个数.(n>=1的整数)
//     * sortBy
//     * 第一个参数是一个函数，该函数的也有一个带T泛型的参数，返回类型和RDD中元素的类型是一致的；
//     * 第二个参数是ascending，从字面的意思大家应该可以猜到，是的，这参数决定排序后RDD中的元素是升序还是降序，默认是true，也就是升序；
//     * 第三个参数是numPartitions，该参数决定排序后的RDD的分区个数，默认排序后的分区个数和排序之前的个数相等，即为this.partitions.size。
//     * */
//    /*
//     * def coalesce(numPartitions:Int, shuffle:Boolean = false)
//     * 返回一个新的RDD，且该RDD的分区个数等于numPartitions个数。如果shuffle设置为true，则会进行shuffle。
//     * */
//
//  }
}