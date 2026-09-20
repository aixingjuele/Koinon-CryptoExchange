# 资深 Java 架构师 (Web3/交易系统方向) 面试宝典

> 💡 **使用指南**：本文档基于您 **14年研发与架构经验**、**Web3数字资产/撮合引擎/MPC钱包** 的背景量身定制。重点聚焦 Java 高级特性（并发/JVM/Netty）、主流中间件进阶原理，以及如何将这些技术与交易系统实战结合，助您在高级/资深岗位面试中展现架构师底蕴。

---

## 目录
1. [一、Java 核心与底层 (并发编程 / JVM / Netty)](#一-java-核心与底层)
2. [二、数据库与持久化 (MySQL 高阶原理)](#二-数据库与持久化)
3. [三、分布式中间件 (Kafka / Redis)](#三-分布式中间件)
4. [四、微服务与架构框架 (Spring / 分布式架构)](#四-微服务与架构框架)
5. [五、结合简历的实战项目答题话术 (高价值)](#五-结合简历的实战项目答题话术)

---

## 一、Java 核心与底层

结合您在“撮合引擎”与“高并发”场景的经验，面试官会重点考察并发和性能调优。

### 1. 并发编程 (JUC)
*   **AQS (AbstractQueuedSynchronizer) 原理**：
    *   **核心**：`volatile int state` + FIFO 双向链表（CLH 队列）。
    *   **应用**：`ReentrantLock`、`CountDownLatch`、`Semaphore` 的底层基石。面试时强调公平锁/非公平锁在 `tryAcquire` 时的区别（非公平锁上来先 CAS 抢一次状态）。
*   **synchronized vs ReentrantLock**：
    *   `synchronized` 是 JVM 层面的锁（锁升级：无锁 -> 偏向锁 -> 轻量级锁 -> 重量级锁）。适合锁竞争不激烈的场景。
    *   `ReentrantLock` 是 API 层面的锁，支持响应中断、超时尝试（`tryLock`）、公平锁。**在撮合引擎中，为了极致性能通常使用细粒度的对象锁（如锁定具体交易对的订单簿）。**
*   **线程池 (ThreadPoolExecutor)**：
    *   **核心参数**：`corePoolSize`, `maximumPoolSize`, `keepAliveTime`, `workQueue`, `RejectedExecutionHandler`。
    *   **工作流程**：核心线程满 -> 进队列 -> 队列满 -> 创最大线程 -> 最大线程满 -> 拒绝策略。
    *   **实战坑点**：清算系统如果用无界队列 `LinkedBlockingQueue` 容易导致 OOM；如果用 `AbortPolicy` 拒绝策略，任务丢失怎么兜底？（答：依赖 Kafka 重新消费或对账补偿）。

### 2. JVM 调优与排障
*   **内存模型 (JMM)**：堆（新生代、老年代）、方法区（元空间）、虚拟机栈、程序计数器。
*   **垃圾回收 (GC)**：
    *   **G1 GC**：将堆划分为多个 Region，可预测停顿时间（MaxGCPauseMillis）。非常适合撮合引擎这种需要低延迟、大内存的场景。
    *   **ZGC**：几乎无 STW（<1ms），适合对延迟要求极高的金融交易系统。
*   **调优与排障实战**：
    *   排查 CPU 飙高：`top -Hp` 找线程，`printf "%x"` 转十六进制，`jstack` 找堆栈（看是否在无限循环或频繁 Full GC）。
    *   排查 OOM：启动参数加上 `-XX:+HeapDumpOnOutOfMemoryError`，拿到 dump 文件用 MAT / VisualVM 分析大对象泄露（比如堆积的订单队列未释放）。

### 3. Netty 网络编程
> 简历中提到了精通 Netty，面试必问长连接推送与行情系统。
*   **Reactor 线程模型**：BossGroup 负责 accept 连接，WorkerGroup 负责 IO 读写。
*   **零拷贝 (Zero-Copy)**：Netty 的 `ByteBuf` 支持切片（Slice）和组合（CompositeByteBuf），避免内存数据在用户态和内核态之间来回拷贝；底层使用 `FileChannel.transferTo()`（如 Kafka 也是用此技术）。
*   **粘包/半包处理**：使用 `LengthFieldBasedFrameDecoder`（基于长度字段的解码器）来解决自定义二进制协议的包边界问题。
*   **实战场景**：在 Market 行情推送中，使用 `ChannelGroup` 管理同一个交易对的订阅者，实现 O(1) 级别的广播推送。

---

## 二、数据库与持久化

### 1. MySQL 高阶原理
*   **事务隔离级别与 MVCC**：
    *   RR（可重复读）是默认级别，通过 MVCC（多版本并发控制：Read View + Undo Log）解决不可重复读。
    *   **幻读怎么解决**？快照读靠 MVCC；当前读（`SELECT ... FOR UPDATE`）靠 **Next-Key Lock（间隙锁 + 记录锁）**。
*   **锁机制（资金安全的基石）**：
    *   在钱包清算时，一定要强调 `SELECT * FROM wallet WHERE id=? FOR UPDATE` 行锁的作用。
    *   **原子扣减防超扣**：`UPDATE wallet SET balance = balance - amount WHERE id=? AND balance >= amount`（乐观锁思想，避免先查后改的并发漏洞）。
*   **索引优化**：
    *   B+ 树结构，非叶子节点只存键，叶子节点存数据（聚集索引）或主键（非聚集索引/二级索引）。
    *   **覆盖索引**与**最左前缀匹配原则**。亿级交易流水表的分库分表（ShardingSphere）及联合索引设计。

---

## 三、分布式中间件

### 1. Kafka (消息队列)
*   **高性能原理**：顺序写磁盘、PageCache、Zero-Copy (sendfile)、批量发送与压缩。
*   **如何保证顺序消费**？
    *   撮合引擎的核心：将同一个交易对（Symbol）作为 Partition Key。保证 BTC/USDT 的下单/撤单都在同一个 Partition 内，Consumer 单线程处理该 Partition，实现天然无锁串行化。
*   **消息不丢（Exactly-Once / At-Least-Once）**：
    *   生产者：`acks=all`，`retries=MAX`。
    *   Broker：多副本（`min.insync.replicas=2`）。
    *   消费者：关闭自动提交（`enable.auto.commit=false`），业务处理完（如落库成功）再手动 ACK。
*   **Kafka 消息积压怎么处理**？
    *   增加 Partition 数量，同时扩容 Consumer 节点。如果是单 Partition 慢（比如 DB 清算慢），Consumer 内可以拉取后放入线程池并发处理（但需注意 DB 行锁防乱序）。

### 2. Redis (缓存与分布式锁)
*   **分布式锁**：`SETNX key value EX 10`。
    *   坑点：锁超时任务没执行完（用 Redisson 看门狗自动续期）。
    *   坑点：主从切换锁丢失（引出 DB 唯一键兜底的架构思想）。
*   **数据结构底层**：ZSet 底层是跳表（SkipList）+ 字典。常用于行情 K 线的热点数据或重试队列（按时间戳排序）。
*   **缓存三大问题**：
    *   穿透：查询不存在的数据（解法：布隆过滤器 / 缓存空对象）。
    *   击穿：热点 Key 过期，并发打满 DB（解法：互斥锁 / 逻辑过期不过期）。
    *   雪崩：大量 Key 同时过期或 Redis 宕机（解法：随机过期时间 / 高可用架构）。

---

## 四、微服务与架构框架

### 1. Spring 生态
*   **Spring IOC & AOP 底层**：
    *   AOP 底层基于动态代理（接口用 JDK 动态代理，类用 CGLIB）。用于日志、事务管理（`@Transactional`）。
    *   **事务失效场景**：方法非 public、内部方法自调用（没走代理对象）、异常被 catch 吞了没重抛（非常容易出生产事故！）。
*   **Spring Boot 自动装配**：`@EnableAutoConfiguration`，通过 `META-INF/spring.factories`（或 2.7 之后的 `org.springframework.boot.autoconfigure.AutoConfiguration.imports`）加载第三方 Starter。

### 2. 分布式系统理论
*   **CAP 与 BASE**：交易清算系统追求 CP 还是 AP？通常是 AP + 最终一致性。
*   **分布式事务**：
    *   2PC / 3PC、TCC、Saga。
    *   **最常用：可靠消息最终一致性（本地消息表）**。撮合引擎产出结果 -> 发 Kafka -> 下游清算扣钱，依靠重试和对账保证最终一致。

---

## 五、结合简历的实战项目答题话术 (高价值)

您有 14 年经验，面试重点绝对不是死记硬背八股文，而是**用八股文解释你做过的复杂架构**。

### 1. 自我介绍切入点 (30-45秒)
> "面试官您好，我叫谢兴旺，有 14 年后端与架构经验。近几年深耕 Web3 与数字资产交易领域，主导了交易所撮合引擎、清算系统、MPC 托管钱包等核心系统的 0-1 建设。
> 在技术栈上，我精通 Java 高并发与 JVM 调优，熟练使用 Netty 处理低延迟行情推送；同时具备 Go 和 Solidity 多语言能力。擅长处理分布式架构中的数据一致性、高并发消息处理等复杂难题。希望能借此机会与您深入交流。"

### 2. 实战场景：撮合引擎的高性能与并发怎么做？
> **答题话术**："撮合引擎对延迟要求极高。我们采用 **内存撮合 + Kafka 异步解耦** 的架构。
> 为了避免并发加锁导致的性能损耗，我们在上游将同一个交易对（Symbol）作为 Kafka 的 Partition Key，保证同一交易对的订单**串行**进入消费端。在内存中，用 `TreeMap` 维护红黑树订单簿，整个撮合过程（除了细粒度的订单簿读写锁）全是纯内存操作，微秒级完成。撮合结果再异步发送 Kafka 给下游的清算和行情模块，彻底把『计算』和『IO』分离开来。"

### 3. 实战场景：钱包与清算的资金一致性怎么保证？
> **答题话术**："在处理充提币或交易清算时，资金安全是底线。我主要通过三层来保证：
> 1. **代码与 DB 层**：绝对不使用先查后改。所有余额变更必须用带有 `WHERE balance >= amount` 的条件原子 UPDATE 语句，利用 InnoDB 行锁防止并发超扣。
> 2. **事务控制**：注意 Spring `@Transactional` 的边界，核心资金操作如果抛出异常绝不内部吞掉，保证完全回滚。
> 3. **架构兜底层**：由于涉及跨服务（撮合发 Kafka 给清算），如果消息丢失，我们有**定时对账任务**。通过比对撮合交易日志、订单表和钱包流水表，如果发现不一致自动触发补偿或人工介入，这是资金安全的最后防线。"

### 4. 实战场景：海量行情推送怎么做？
> **答题话术**："行情和盘口推送具有高频、多端的特点。我们在后端采用了 Netty 构建长连接网关。
> 为了精准路由，我们使用了基于 Topic 的订阅模型。用户建连后，将 Channel 按 `Topic (如 BTC/USDT)` 存入内存的 ChannelGroup 中。当 Kafka 消费到新盘口时，直接找到对应的 ChannelGroup 批量 `writeAndFlush`。对于多节点部署，结合 Redis Pub/Sub 实现跨节点的内部广播，确保所有网关节点都能将行情下发给连接到本机的客户端。"