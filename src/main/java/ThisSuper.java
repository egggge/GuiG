class test {
    public static  void port(String s){
        System.out.println(s);
    }
    test(){
        port("第一个构造方法");
    }
    test(String s){
        port("第二个构造方法"+s);

    }
}
public class ThisSuper extends test{
    ThisSuper(){
        super();
        port("子类的第一个构造方法");
    }
    ThisSuper(String s){
        super(s);
        port("子类的第二个构造函数"+s);
    }
    ThisSuper(String S,Integer I){
        this(S);//这个是相当于调用子类的一个构造函数
        port("调用子类的第三个构造函数"+I);
    }
    public static void main(String[] args){
        System.out.println("---------------");
        ThisSuper t = new ThisSuper();
        System.out.println("---------------");
        t = new ThisSuper("AA");
        System.out.println("---------------");
        t = new ThisSuper("BB",12);
    }
}
