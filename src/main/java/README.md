## day 3 (2026.04.11)

### 今日目标
-[x] Box<T> 泛型类
-[x] 泛型类方法swap(Box<T> a,Box<T> b)
-[x] SimpleStream<T>模拟Flink链式调用(map + filter)

### 关键理解
-[x] java中Box<T> 可对照 DataStream<T>
-[x] 泛型类方法swap<T> 可对照 map<R>(),filter<T>()
-[x] 方法返回this/新对象 可对照 链式调用 stream.map().filter()

### 代码文件
- Box.java
- SimpleStream.java

### 明日计划
- 类型擦除 (理解泛型底层)
- 开始Flink DataStream API 实战
