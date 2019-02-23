import java.text.SimpleDateFormat;
import java.util.*;

public class tryData {
    public static void dataAbout(){
        Date date = new Date();
        System.out.println(date.toString());
        SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss");
        System.out.println("当前时间为: " + ft.format(date));
    }
    public  static void calendarAbout(){
        Calendar c1 = Calendar.getInstance();
        //指定一个特定的时间
//        c.set(2009, 6 - 1, 12);
//        System.out.println(c);
        // 获得年份
        int year = c1.get(Calendar.YEAR);
        System.out.println(c1.getTime());
        // 获得月份
        int month = c1.get(Calendar.MONTH) + 1;
        // 获得日期
        int date = c1.get(Calendar.DATE);
        // 获得小时
        int hour = c1.get(Calendar.HOUR_OF_DAY);
        // 获得分钟
        int minute = c1.get(Calendar.MINUTE);
        // 获得秒
        int second = c1.get(Calendar.SECOND);
        // 获得星期几（注意（这个与Date类是不同的）：1代表星期日、2代表星期1、3代表星期二，以此类推）
        int day = c1.get(Calendar.DAY_OF_WEEK);

    }
    public static void main(String[] args){
        //dataAbout();
        calendarAbout();
    }

}
