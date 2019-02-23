import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.List;

public class tryJson {

    public static void main(String[] args) {
        ArrtoList();
        //解析Json--传入Json字符串
//        JSONObject my_json=new JSONObject(CreateJson());
//        String total=my_json.getString("total");
//        System.out.println("总数为："+total);
//
//        String class_name=my_json.getString("class");
//        System.out.println("班级为："+class_name);
//
//        JSONArray members=my_json.getJSONArray("members");
//        for(int i=0;i<members.size();i++)
//        {
//            JSONObject member=(JSONObject)members.get(i);
//            System.out.println("姓名："+member.getString("name")+" 年龄："+member.getString("age")
//                    +" 成绩："+member.getString("score"));
//        }

    }
    public static JSONObject CreateJson()
        {
        JSONObject my_json = new JSONObject();
        my_json.put("class","二年级");
        my_json.put("total",2);
        JSONArray members = new JSONArray();

        //第一个同学
        JSONObject member1=new JSONObject();
        member1.put("name", "李小红");
        member1.put("age", 18);
        member1.put("score", 95);
        members.add(member1);
        //第二个同学
        JSONObject member2=new JSONObject();
        member2.put("name", "王尼玛");
        member2.put("age", 28);
        member2.put("score", 85);
        members.add(member2);
        my_json.put("members", members);
        return my_json;

    }
    public static void ArrtoList(){
        String[] s = {"aa","bb","cc"};
        List<String> strlist = Arrays.asList(s);
        for(String str:strlist){
            System.out.println(str);
        }
        System.out.println("------------------------");
        //基本数据类型结果打印为一个元素
        int[] i ={11,22,33};
        List intlist = Arrays.asList(i);
        for(Object o:intlist){
            System.out.println(o.toString());
        }
        System.out.println("------------------------");
        Integer[] ob = {11,22,33};
        List<Integer> oblist = Arrays.asList(ob);
        for(int a:oblist){
            System.out.println(a);
        }
        System.out.println("------------------------");
    }
}
