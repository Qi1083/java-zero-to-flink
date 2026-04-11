import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;

// 定义接口
interface Function<T, R> {
    R apply(T t);
}

interface Predicate<T> {
    boolean test(T t);
}

public class SimpleStream<T> {
    private T data;

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public SimpleStream(T data) {
        this.data = data;
    }

    // 转换操作，返回新的SimpleStream
    public <R> SimpleStream<R> map(Function<T, R> mapper) {
        R result = mapper.apply(data); // 就近原则，data在没有变量冲突的情况下可以省去this.
        return new SimpleStream<>(result);
    }

    // 过滤操作
    public SimpleStream<T> filter(Predicate<T> predicate) {
        if (predicate.test(data)) {
            return this;
        }
        return null;
    }

    public void print() {
        System.out.println("Result: " + data);
    }

    public static void main(String[] args) {
        SimpleStream<String> simple = new SimpleStream<>("hello");
        SimpleStream<Tuple2<String, Integer>> tupleWorlds = simple.map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> apply(String s) {
                String worlds = Arrays.toString(s.split(" "));
                return Tuple2.of(worlds, 1);
            }
        });

        SimpleStream<Tuple2<String, Integer>> filter = tupleWorlds.filter(new Predicate<Tuple2<String, Integer>>() {
            @Override
            public boolean test(Tuple2<String, Integer> out) {
                return out.f0.length() >= 10;
            }
        });

        if (filter != null) {
            System.out.println(filter.getData());
        } else {
            System.out.println("数据被过滤！");
        }

        // 等价于
        new SimpleStream<>("hello").map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> apply(String s) {
                String worlds = Arrays.toString(s.split(" "));
                return Tuple2.of(worlds, 1);
            }
        }).filter(new Predicate<Tuple2<String, Integer>>() {
            @Override
            public boolean test(Tuple2<String, Integer> out) {
                return out.f0.length() >= 5;
            }
        }).print();

    }

}
