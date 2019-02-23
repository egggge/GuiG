import com.mongodb.BasicDBObject;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

public class OrderAmount {
    /*
    第一步：筛选出action_type=6,根据rec_type进行分类
     */
    private static String createQuerySql1() {
        String queryDate = MongoDateUtil.getQueryYesterdayDate();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select uid,item_id,item_position from action where action_type = 6 and day=,rec_type=2 '");
        stringBuilder.append(queryDate);
        //stringBuilder.append("' group by rec_type");
        return stringBuilder.toString();
    }
    private static String createQuerySql2() {
        String queryDate = MongoDateUtil.getQueryYesterdayDate();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select uid,item_id,item_position from action where action_type = 6 and day=,rec_type!=2 '");
        stringBuilder.append(queryDate);
        //stringBuilder.append("' group by rec_type");
        return stringBuilder.toString();
    }
    public static JavaPairRDD positionAmount( Dataset<Row> sqlDf){
        JavaPairRDD<String, String> pairRDD = sqlDf.javaRDD().mapPartitionsToPair(iterator -> {
            List<Tuple2<String, String>> list = new ArrayList<>();
            //统计同一位置的用户订购
            while (iterator.hasNext()) {
                Row row = iterator.next();
                //<item_position,uid>
                list.add(new Tuple2<>(row.getString(2), row.getString(0)));
            }
            return list.iterator();
        });
        return pairRDD;
    }
    public static JavaPairRDD positionAmountAI( Dataset<Row> sqlDf){
        JavaPairRDD<String, String> pairRDD = sqlDf.javaRDD().mapPartitionsToPair(iterator -> {
            List<Tuple2<String, String>> list = new ArrayList<>();
            //统计同一位置的用户订购
            while (iterator.hasNext()) {
                Row row = iterator.next();
                //<item_position,uid>
                list.add(new Tuple2<>(row.getString(1), row.getString(0)));
            }
            return list.iterator();
        });
        return pairRDD;
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("AreaOrderCountApp")
                .config("spark.mongodb.output.uri", Constant.MONGO_HOST + Constant.ORDER_TABLE)
                .master("yarn").enableHiveSupport().getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        String sql1 = createQuerySql1();
        String sql2 = createQuerySql2();
        //获取行为数据下action_type==6的用户行为数据
        Dataset<Row> sqlDf = sparkSession.sql(sql1);
        //根据position对非智能数据进行分类
        JavaPairRDD pairRDD = positionAmount(sqlDf);
        Map<String, Long> map = pairRDD.countByKey();
        //根据position对智能数据进行分类
        Dataset<Row> sqlDfAI = sparkSession.sql(sql2);
        JavaPairRDD pairRDDAI = positionAmountAI(sqlDfAI);
        Map<String, Long> mapAI = pairRDDAI.countByKey();
        // 因为每天凌晨1点后上一天的行为数据才完全导入推荐系统数据库，因此该任务应设置在凌晨1点后执行。
        Date day = MongoDateUtil.getOutYesterdayDate();
        // 将rdd转换为Document类型，为存入Mongodb做准备
        Document doc1 = new Document(new BasicDBObject(map));
        collection.insertOne(doc1);
        JavaRDD<Document> documents = pairRDD.mapPartitions(iterator -> {
            List<Document> documentList = new ArrayList<>();
            while (iterator.hasNext()) {
                Tuple2 pair = iterator.next();
                Document document = new Document("area_code", pair._1);
                document.put("order_sum", pair._2);
                document.put("date",day);
                documentList.add(document);
            }
            return documentList.iterator();
        });
        // 将各地区一天内的订购量存储进Mongodb数据库
        MongoSpark.save(documents);
        jsc.close();
    }
}
