public class Demo {
    // 测试仓库变更
    // 本地远程变更再测
    // 更新项目根目录
    public static void main(String[] args) {
        System.out.println("hello");
        System.out.println("world");
        System.out.println("测试仓库变更");
        System.out.println("本地远程变更再测");


        /*A[] a = new A[1000];
        for (int i = 0; i < 1000; i++) {
            a[i] = new A();
        }*/
    }
    
    public static  class A {
        private byte[] a ;
        public A(){
            a = new byte[256*1024*1024];
        }
    }
}
