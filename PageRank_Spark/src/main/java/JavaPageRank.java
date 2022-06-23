import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import java.util.*;


public final class JavaPageRank {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        if (args.length < 2) {
            System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        conf.setAppName("JavaPageRank");
                //.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaPairRDD<Character,char[]> pairs =lines.mapToPair(new PairFunction<String, Character, char[]>() {
            public Tuple2<Character,char[]> call(String s) {
                String[] str=s.split(" ");
                char[] ch=new char[str.length];
                for (int i=0;i<str.length;i++){
                    ch[i]=str[i].charAt(0);
                }
                return new Tuple2<Character,char[]>(s.charAt(0),ch );
            }//将字符串中保存的页面的链接关系map转换成key-values形式，key当前页面，指向的页面集合用数组表示
        }).cache();//持久化
        JavaPairRDD<Character,Float> ranks=sc.parallelize(Arrays.asList('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z')).mapToPair(new PairFunction<Character, Character, Float>() {
            public Tuple2<Character,Float> call(Character character) throws Exception {
                return new Tuple2<Character,Float>(character,new Float(1.0));
            }//初始化页面权值是1.0
        });
        for(int i=0;i<40;i++){
            JavaPairRDD<Character,Tuple2<char[],Float>> contribs=pairs.join(ranks);
            JavaPairRDD<Character,Float> con=contribs.flatMapToPair(new PairFlatMapFunction<Tuple2<Character,Tuple2<char[],Float>>,Character,Float>(){
                public Iterator call(Tuple2<Character,Tuple2<char[],Float>> val) throws Exception{
                    List<Tuple2<Character,Float>> list=new ArrayList<Tuple2<Character, Float>>();
                    Float f=val._2._2;
                    char[] ch=val._2._1();
                    int len=ch.length;
                    for(int i=0;i<len;i++) {
                        Tuple2<Character, Float> map = new Tuple2<Character, Float>(new Character(ch[i]), new Float(f / len));
                        list.add(map);
                    }
                    return list.iterator();
                }
            });//将每个页面获得其他页面的pagerank值形成键值对的形式
            ranks=con.reduceByKey(new Function2<Float, Float, Float>() {
                public Float call(Float a, Float b) { return a + b; }
            }).mapValues(new Function<Float, Float>() {
                public Float call(Float a) throws Exception {
                    return new Float(0.15*1.0/26+0.85*a);
                }
            });//当前迭代的pagerank计算
        }
        ranks.saveAsTextFile(args[1]);

        Map map=ranks.collectAsMap();//访问所有页面的pagerank值
        Set set=map.keySet();
        Iterator it=set.iterator();
        while(it.hasNext()){
            System.out.println(map.get(it.next())  );
        }

        long endTime = System.currentTimeMillis();
        long usedTime = endTime-startTime;
        System.out.println(usedTime);

        sc.stop();

//        //JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
//
//        /** links 是(String ,Iterable<String>)
//         *  先将数据转为key value形式的RDD，然后根据key进行分组  key是某一个url  value是这个key的的所有邻居url组成的集合
//         */
//        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(
//                new PairFunction<String, String, String>() {
//                    @Override
//                    public Tuple2<String, String> call(String s) {
//                        String[] parts = SPACES.split(s);
//                        return new Tuple2<>(parts[0], parts[1]);
//                    }
//                }).distinct().groupByKey().cache();
//
//
//        // ranks的初始形式是(String ,Double)其中double一列初始都是1
//        JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
//            @Override
//            public Double call(Iterable<String> rs) {
//                return 1.0;
//            }
//        });
//
//
//        for (int current = 0; current < 26; current++) {
//            // Calculates URL contributions to the rank of other URLs.
//            // links 是(String ,Iterable<String>) ranks是(String ,Double)  join之后是( String ,（Iterable<String>，Double）)
//            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
//                    .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
//                        @Override
//                        //对values进行flatMapToPair操作  ，value的格式Tuple2<Iterable<String>, Double>
//                        //迭代元组中第一个元素 然后权重分别设置为第二个元素除以第一个元素的里面url的个数
//                        //组成二元组Tuple2<String, Double>  ，因为第一个元素里面有多个url会迭代
//                        // 所以call方法返回多个Tuple2<String, Double> 组成的一个Iterator，
//                        // 最后会进行扁平化，所以flatMapToPair返回了JavaPairRDD<String, Double>
//                        public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
//                            int urlCount = Iterables.size(s._1);
//                            List<Tuple2<String, Double>> results = new ArrayList<>();
//                            for (String n : s._1) {
//                                results.add(new Tuple2<>(n, s._2() / urlCount));
//                            }
//                            return results.iterator();
//                        }
//                    });
//
//
//            //contribs是JavaPairRDD<String, Double>  然后我们进行根据key操作对value求和
//            //那么得到了key及其自己对应的权重值，然后得到平滑后的每个url对应的权重得到新的ranks
//            //继续迭代，继续把links跟ranks进行join然后更新权重
//            ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
//                @Override
//                public Double call(Double sum) {
//                    return 0.15 + sum * 0.85;
//                }
//            });
//        }
//
//        ranks.saveAsTextFile(args[1]);
//
//        //进行输出ranks存放了url及其对应的权重
//        List<Tuple2<String, Double>> output = ranks.collect();
//        for (Tuple2<?,?> tuple : output) {
//            System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
//        }
//
//        sc.stop();
    }
}
