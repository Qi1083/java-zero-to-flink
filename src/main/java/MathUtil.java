public class MathUtil {
    public static int add(int a, int b) {
        return a + b;
    }

    public int multiply(int a, int b) {
        return a * b;
    }

    public static void main(String[] args) {
        System.out.println(MathUtil.add(1,2));

        MathUtil mul=new MathUtil();
        System.out.println(mul.add(1,2));
        System.out.println(mul.multiply(2,3));

    }
}
