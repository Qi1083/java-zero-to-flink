# Java 基础学习 - 实时数据转型

## Day 2（2026.04.10）

### 今日目标
理解 Java 静态/实例方法 和 匿名内部类，建立 Flink API 的语法基础。

### 完成内容
- [x] MathUtil.java：静态方法 `add()` vs 实例方法 `multiply()`
- [x] 理解 `env.readTextFile()` 的调用方式（对象.实例方法）
- [x] Button.java：匿名内部类实现 `ClickListener` 接口
- [x] 识别 Flink WordCount 中的 2 处匿名内部类用法

### 关键理解
| Java 概念 | Flink 对应 |
|-----------|-----------|
| 静态方法 `MathUtil.add()` | `StreamExecutionEnvironment.getExecutionEnvironment()` |
| 实例方法 `util.multiply()` | `env.readTextFile("file.txt")` |
| 匿名内部类 `new ClickListener(){}` | `new FlatMapFunction(){}` |

### 代码文件
- `MathUtil.java`：静态 vs 实例方法对比
- `Button.java`：匿名内部类回调机制
- `WorldCountStreamDemo.java`：标注匿名内部类位置（2处）

### 明日计划
- 泛型方法：`Box<T>` 与 `DataStream<T>`
- 用 Java 模拟 Flink 的链式调用

---

**状态**：在职学习，32岁转型实时数据工程师。


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
