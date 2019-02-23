import java.util.*;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
public class tryMongdb {
    public static void test(){
        String[] word = {"hello:1","word:2","a:3"};
        List list = Arrays.asList(word);
        System.out.println(list);

    }

    public static void main( String args[] ){
        try{
            // 连接到 mongodb 服务
            MongoClient mongoClient = new MongoClient( "127.0.0.1" , 27017 );

            // 连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");
            System.out.println("Connect to database successfully");

            //创建集合
//            mongoDatabase.createCollection("test");
//            System.out.println("集合创建成功");

            MongoCollection<Document> collection = mongoDatabase.getCollection("test");
            System.out.println("集合 test 选择成功");
            //插入文档
            /**
             * 1. 创建文档 org.bson.Document 参数为key-value的格式
             * 2. 创建文档集合List<Document>
             * 3. 将文档集合插入数据库集合中 mongoCollection.insertMany(List<Document>) 插入单个文档可以用 mongoCollection.insertOne(Document)
             * */
//            Document document = new Document("uid","fly").append("uid","fly").append("uid",100).append("uid","fly2");
//            List<Document> documents = new ArrayList<Document>();
//            documents.add(document);
//            collection.insertMany(documents);
//            collection.insertOne(new Document("_id", 1).append("uid", "uid").append("group_id", "group_doc"));
//            Document doc =new Document("test","hello");
//            System.out.println("文档插入成功");

//            //更新文档   将文档中likes=100的文档修改为likes=200
//            collection.updateMany(Filters.eq("likes", 100), new Document("$set",new Document("likes",200)));
            BasicDBObject query = new BasicDBObject();
            query.put("by", "fly");
            Document result = collection.findOneAndUpdate(query, new Document("$inc", new Document("likes", 1)));
            int id = result.getInteger("likes").intValue();
            System.out.println(id);//输出201
//            //删除符合条件的第一个文档
//            collection.deleteOne(Filters.eq("likes", 200));
//            //删除所有符合条件的文档
//            collection.deleteMany (Filters.eq("likes", 200));
              //查找文档
//            BasicDBObject query = new BasicDBObject();
//            query.put("uid", "fly");
//            FindIterable<Document> doc = collection.find(query);
//            System.out.println(doc.first());

            //检索所有文档
            /**
             * 1. 获取迭代器FindIterable<Document>
             * 2. 获取游标MongoCursor<Document>
             * 3. 通过游标遍历检索出的文档集合
             * */
            FindIterable<Document> findIterable = collection.find();
            MongoCursor<Document> mongoCursor = findIterable.iterator();
            while (mongoCursor.hasNext()){
                System.out.println(mongoCursor.next());
            }



        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }

}
