public class Box<T> {
    private T item;

    private void set(T item) {
        this.item = item;
    }

    public T get() {
        return this.item;
    }

    // 泛型方法 交换两个box 第一个<T>声明这个方法支持泛型 T
    public static <T> void swap(Box<T> a, Box<T> b) {
        T temp = a.get();
        a.set(b.get());
        b.set(temp);
    }

    public static void main(String[] args) {
        Box<String> b1 = new Box<>();
        b1.set("hello");
        System.out.println(b1.get());

        System.out.println("##########################");

        Box<Integer> b2 = new Box<>();
        b2.set(123);

        Box<Integer> b3 = new Box<>();
        b3.set(456);

        System.out.println("交换前：" + b2.get() + "," + b3.get());

        // Box.swap(b1,b2); 会报错
        // swap(b2, b3);
        Box.swap(b2, b3);

        System.out.println("交换后：" + b2.get() + "," + b3.get());

        System.out.println("##########################");

        // JVM 看Box<String> 和 Box<Integer> 是两个不同类型吗
        System.out.println(b1.item.getClass());
        System.out.println(b1.getClass());
        System.out.println(b1.getClass().equals(b2.getClass()));  // 返回为True 证明类型被擦除


        if (b1 instanceof Box){
            System.out.println("True");
        }

    }

}
