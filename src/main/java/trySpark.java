import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class trySpark {
//    class ValueComparator implements Comparator<String>,Serializable {
//        //根据Map的值进行比较
//        @Override
//        public int compare(String a,String b) {
//            //int r = base.get(a).compareTo(base.get(b));
//            //if (r == 0) return 1; // 不这样写，值相同的会被删掉；但是这样写，get会返回null。看自己的需求写吧。
//
//
//        }
//    }
    public static void filterFun(){
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\sample.txt");
        JavaRDD<String> zksRDD = lines.filter(new org.apache.spark.api.java.function.Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("zks");
            }
        });
        //打印内容
        List<String> zksCollect = zksRDD.collect();
        //List的两种打印方式
        for (String str:zksCollect){System.out.println(str);}
        //使用迭代器
        Iterator it = zksCollect.iterator();
        while(it.hasNext()) {
            System.out.println(it.next());
        }
        sc.close();

    }

    /*
    统计活跃用户
     */
    public  JavaRDD<Document> TopClick() {
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\test.txt");
        JavaRDD<Document> documentJavaRdd = null;
        final JavaPairRDD<String, String> pairRDD = lines.mapPartitionsToPair(it -> {
            List<Tuple2<String, String>> tuple2s = new ArrayList<>();
            while (it.hasNext()) {
                String row = it.next();
                String itemId = row.split(":")[0];
                String userId = row.split(":")[1];
                tuple2s.add(new Tuple2<>(itemId, userId));
            }
            return tuple2s.iterator();
        });
        Map<String, Long> map = pairRDD.distinct().countByKey();
        System.out.println(map);
        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return o2.getValue().compareTo(o1.getValue()); // 降序
                //return o1.getValue().compareTo(o2.getValue()); // 升序
            }
        });
        //打印内容
        for (Map.Entry<String, Long> m : list.subList(0,2)){
            System.out.println("key:" + m.getKey() + ",values:" + m.getValue());
            }

        JavaRDD<Map.Entry<String, Long>> topTenRdd = sc.parallelize(list.subList(0,1));
        documentJavaRdd = topTenRdd.mapPartitions(MapIterator -> {
            List<Document> documents = new ArrayList<>();
            while (MapIterator.hasNext()) {
                Map.Entry<String, Long> maps  = MapIterator.next();
                Document document = new Document("category", 1);
                document.put("item_id", ((Map.Entry) maps).getKey());
                document.put("watch_duration", ((Map.Entry) maps).getValue());
                //document.put("date", date);
                documents.add(document);
            }
            return documents.iterator();
        });
        return documentJavaRdd;
}

    public static void ClickTop() {
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\test.txt");
        final JavaPairRDD<String, String> pairRDD = lines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(":")[0], s.split(":")[1]);
            }
        });
        Map<String, Long> Clickcount = pairRDD.distinct().countByKey();
        for (Map.Entry<String, Long> map : Clickcount.entrySet()) {
            System.out.println("key:" + map.getKey() + ",values:" + map.getValue());

        }
    }
    public static void mapToPairFun(){
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\sample.txt");

        final JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split(":")[0],1); }
            }).distinct().reduceByKey((x, y) -> x + y);
        //打印内容
        List<Tuple2<String, Integer>> output = pairRDD.collect();
        System.out.println("输出结果"+(output.toString()));

    }
    public static void flatmaptopairFun(){
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\sample.txt");
        //分割文件，按照规定把字符分割开来，生成数组
//        lines.map(x->x.split("\n")).foreach(y->{
//            for(String s:y){
//                System.out.println(s);
//            }
//        });
        //相邻字符出现次数统计
//        lines.map(x->x.split(" ")).flatMapToPair(y->{
//            List<Tuple2<String,Integer>> list = new ArrayList<>();
//            for(Integer index=0;index<y.length-1;index++){
//                String key = y[index]+y[index+1];
//                list.add(new Tuple2<>(key,1));
//            }
//            return list.iterator();
//    }).reduceByKey((m,n)->m+n).foreach(p->System.out.printf(p._1+p._2));
//        JavaPairRDD<String, Integer> wordPairRDD = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
//            @Override
//            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
//                ArrayList<Tuple2<String, Integer>> tpLists = new ArrayList<Tuple2<String, Integer>>();
//                String[] split = s.split("\n");
//                for (int i = 0; i <split.length ; i++) {
//                    Tuple2 tp = new Tuple2<String,Integer>(split[i], 1);
//                    tpLists.add(tp);
//                }
//                return tpLists.iterator();
//            }
//        });
//        wordPairRDD.foreach(y->System.out.println(y._1+y._2));
    }

    public static void mapFun(){
        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\sample.txt");
        JavaRDD<Iterable<String>> mapRDD = lines.map(new org.apache.spark.api.java.function.Function<String, Iterable<String>>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split);
            }
        });
        //读取第一个元素
        System.out.println("输出结果是：\n"+mapRDD.first());

    }
    public static void Test09StreamMapFlatMap(){
        /**
         * Stream的map和flatMap的区别:
         * map会将一个元素变成一个新的Stream
         * 但是flatMap会将结果打平，得到一个单个元素
         /**获取单词，并且去重**/
        List<String> list = Arrays.asList("hello welcome", "world hello", "hello world",
                "hello world welcome");

        //map和flatmap的区别
        list.stream().map(item -> Arrays.stream(item.split(" "))).distinct().collect(Collectors.toList()).forEach(System.out::println);
        System.out.println("---------- ");
        list.stream().flatMap(item -> Arrays.stream(item.split(" "))).distinct().collect(Collectors.toList()).forEach(System.out::println);

        //实际上返回的类似是不同的
        List<Stream<String>> listResult = list.stream().map(item -> Arrays.stream(item.split(" "))).distinct().collect(Collectors.toList());
        List<String> listResult2 = list.stream().flatMap(item -> Arrays.stream(item.split(" "))).distinct().collect(Collectors.toList());

        System.out.println("---------- ");

        //也可以这样
        list.stream().map(item -> item.split(" ")).flatMap(Arrays::stream).distinct().collect(Collectors.toList()).forEach(System.out::println);

        System.out.println("================================================");

        /**相互组合**/
        List<String> list2 = Arrays.asList("hello", "hi", "你好");
        List<String> list3 = Arrays.asList("zhangsan", "lisi", "wangwu", "zhaoliu");

        list2.stream().map(item -> list3.stream().map(item2 -> item + " " + item2)).collect(Collectors.toList()).forEach(System.out::println);
        list2.stream().flatMap(item -> list3.stream().map(item2 -> item + " " + item2)).collect(Collectors.toList()).forEach(System.out::println);

        //实际上返回的类似是不同的
        List<Stream<String>> list2Result = list2.stream().map(item -> list3.stream().map(item2 -> item + " " + item2)).collect(Collectors.toList());
        List<String> list2Result2 = list2.stream().flatMap(item -> list3.stream().map(item2 -> item + " " + item2)).collect(Collectors.toList());

    }
    public static void forFun(){
        List<String>list=new ArrayList<>();
        list.add("1");
        list.add("make");//add是将传入的参数当String看，哪怕传入一个很长的list，也只算一个
        list.add("2");
        System.out.println("巅峰时期的list"+list);
        list.remove(0);//list中，remove后接的是索引
        System.out.println("失去了第一个数的list"+list);
        HashSet<String>set=new HashSet<>();
        set.add("make");
        System.out.println("set包含了"+set);
        System.out.println("list是否包含set   "+list.containsAll(set));//list和set是可以相互包含的
        list.removeAll(set);
        System.out.println("removeAll(set)之后的list"+list);
        list.addAll(set);//addAll是将传入的参数当list看，有多少加多少,类似的removeAll,containsAll都如此
        System.out.println("addAll(set)之后的list"+list);
//
        list.add("jack");//add只表示加入一个字符串
        list.add("howk");
        //在java集合中，使用专门的迭代器进行遍历
        Iterator<String>it=list.iterator();
        System.out.println("添加了jack和howk后，现在list的元素包含：");
////		以前老套的迭代器做法
		while(it.hasNext()) {
			System.out.print(it.next()+" ");
		}
        System.out.println("非常高逼格的使用迭代器Lambda表达式遍历");
        it.forEachRemaining(String->System.out.print(String+" "));
        System.out.println();

        System.out.println("第二次高逼格，显然已经输不出来了");
        it.forEachRemaining(String->System.out.print(String+" "));

        System.out.println();
        System.out.println("我foreach的迭代元素为");
        //对于list中的每一个String，我都要输出,帅！

        list.forEach(String->System.out.print(String+" "));//Lambda表达式
        System.out.println();
//		forEach和forEachRemaining区别不大，可以换着用，但是这个第二次能输出来
        System.out.println("我Foreach还可以来第二次");
        list.forEach(String->System.out.print(String+" "));//Lambda表达式
        System.out.println();

        System.out.println(list.get(0)+"的长度是"+list.get(0).length());
        System.out.println(list.get(1)+"的长度是"+list.get(1).length());
        System.out.println(list.get(2)+"的长度是"+list.get(2).length());

        //删除list中的所有长度大于2的字符串型元素
        list.removeIf(ele->((String)ele).length()>2);

        System.out.println("用removeIf取出长度大于2的字符串后list为"+list);

    }

    //spark编程进阶
    //未完待续
    public static void squareFun(){

        SparkConf conf = new SparkConf().setAppName("SparkRDD").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> lines = sc.textFile("D:\\letter.txt");
        List<Integer> list = new ArrayList<Integer>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        //初始化R
        JavaRDD<Integer> rdd = sc.parallelize(list);
        //第二和第三个参数为函数的匿名实现（lambda形式 ）
        Tuple2<Double, Integer> t = rdd.aggregate(new Tuple2<Double, Integer>(0.0, 0),
                (x,y)->new Tuple2<Double, Integer>(x._1+y,x._2+1),
                (x,y)->new Tuple2<Double, Integer>(x._1+y._1,x._2+y._2));
        System.out.println(t._1/t._2);

    }


    public static void main(String[] args) throws Exception{
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mycol.test")
                .getOrCreate();

        trySpark trySpark = new trySpark();
        JavaRDD<Document> documentJavaRDD = trySpark.TopClick();
        if (documentJavaRDD != null) {
            MongoSpark.save(documentJavaRDD);
        }

    }
    }



