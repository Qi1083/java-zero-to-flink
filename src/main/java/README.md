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

## Day 4（2026.04.13）

### 今日目标
独立写Flink WordCount，理解类型擦除

### 完成内容
- [x] Flink WordCount完整实现（临摹→独立写→对照）
- [x] 类型擦除理解：运行时`Box<String>`和`Box<Integer>`是同一个类
- [x] 掌握泛型限制：不能`instanceof T`，不能`new T()`

### 关键理解
| 概念 | 说明                               |
|------|----------------------------------|
| DataStreamSource<T> | extends DataStream<T>，泛型参数在运行期擦除 |
| 类型擦除 | 编译期检查类型安全，编译期生成字节码时变成原始类型        |
| 匿名内部类 | flatMap/map/keyBy中大量使用，实现函数式接口   |

### 代码文件
- WorldCount.java（独立实现）

### 明日计划
- Flink算子深入：flatMap vs map vs filter
- 状态管理概念

## Day 5（2026.04.14）

### 今日目标
理解map、flatmap、filter的区别，理解状态管理基础

### 完成内容
- [x] map、flatmap、filter代码对照
- [x] 状态管理基础示例

### 关键理解
算子	    作用	        输入条数	      输出条数	         效果
map	    一对一转换	1 条	       1 条	            变个样子
filter	过滤	        1 条	    0 条 或 1 条	        留下符合条件的
flatMap	一对多展开	1 条	    0 条 / 1 条 / 多条	拆开、打散、输出多个

特性	      普通局部变量	Flink ValueState 状态
跨数据保存	❌ 不保存	   ✅ 永久保存
key 隔离	    ❌ 共用	       ✅ 每个 key 独立存储
容错恢复	    ❌ 重启丢失	   ✅ 自动恢复
多并行度安全	❌ 线程不安全	   ✅ 框架管理，安全
过期清理	    ❌ 不能	       ✅ 支持 TTL 自动清理

### 代码文件
- MapFilterFlatMap.java（独立实现）
- KeyedValueStateDemo.java（参照完成）

### 明日计划
- Flink集群架构
