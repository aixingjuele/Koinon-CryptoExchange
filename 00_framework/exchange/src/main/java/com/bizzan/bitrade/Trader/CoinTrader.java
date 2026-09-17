package com.bizzan.bitrade.Trader;

import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.entity.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 【核心类·面试必看】单个交易对的内存撮合引擎（每个交易对一个实例，如 BTC/USDT 一个 CoinTrader）
 *
 * ==================== 一、整体架构（先建立全局观）====================
 * 本系统采用"内存撮合 + Kafka 异步解耦"架构，全链路如下：
 *   ① 下单 API(exchange-api/OrderController) → ② DB事务内冻结资金(乐观锁SQL) → ③ 发Kafka(topic=exchange-order)
 *   → ④ 本类内存撮合(exchange模块,ExchangeOrderConsumer单分区顺序消费)
 *   → ⑤ 发Kafka(exchange-trade成交明细 / exchange-order-completed订单完成 / exchange-trade-plate盘口变化)
 *   → ⑥ market模块消费：清算落库(加币扣币) + K线生成 + WebSocket/Netty推送
 *
 * ==================== 二、为什么能"无锁高并发"（核心设计思想）====================
 * 严格说本类并非完全无锁，而是"单线程化 + 防御性细粒度锁"：
 *  1.【单线程撮合】每个交易对一个 CoinTrader，订单经 Kafka 分区串行到达，
 *    理想部署下同一交易对的撮合永远只有一个线程执行（与 LMAX Disruptor 单消费者模型同思想），
 *    因此撮合主流程读写订单簿天然无竞争——这是"无锁"的真正来源：不是用了什么黑科技，
 *    而是通过"串行化"从根上消除了竞争。
 *  2.【细粒度防御锁】代码中的 synchronized(list) 只锁单个方向的订单簿（如只锁买单簿），
 *    锁粒度小、临界区全是纯内存操作（纳秒~微秒级），用于防止"撤单线程"(另一个Kafka topic的
 *    消费线程)与"撮合线程"并发改同一队列。即使两个线程竞争，也几乎不阻塞。
 *  3.【对比业界常见方案】
 *    - 数据库撮合(早期小交易所)：订单簿落库，靠行锁+事务撮合，TPS 几十~几百，延迟高
 *    - 内存撮合+细粒度锁(本方案)：单线程撮合，单交易对理论可达数千~数万 TPS
 *    - LMAX Disruptor 无锁环形队列(币安早期/各大所核心)：连 synchronized 都省掉，
 *      单线程消费 RingBuffer + 内存账本，单机百万级 TPS
 *    - 按交易对分片(纳斯达克/币安)：与本系统"按交易对部署节点"思想完全一致
 *
 * ==================== 三、盘口数据结构（面试必问：盘口怎么实现）====================
 *  买单簿 buyLimitPriceQueue : TreeMap<价格, MergeOrder>，价格【降序】→ firstKey 即"买一"
 *  卖单簿 sellLimitPriceQueue: TreeMap<价格, MergeOrder>，价格【升序】→ firstKey 即"卖一"
 *  MergeOrder：同一价格档位的订单列表(ArrayList)，按到达顺序排列 → 实现"时间优先(FIFO)"
 *  市价单队列 buy/sellMarketQueue：LinkedList，按时间先后排队
 *  TradePlate：前端展示的盘口(买卖各 maxDepth=100 档)，与订单簿【冗余存储、同步维护】，
 *    撮合时直接改盘口，避免每次推送都遍历订单簿聚合（用内存换 CPU）
 *
 *  为什么用 TreeMap 而不是 PriorityQueue(堆)？
 *   - 撤单需要"按价格定位档位"：TreeMap.get(price) 是 O(logN)，堆删除任意元素是 O(N)
 *   - 撮合从最优价开始逐档扫：TreeMap 迭代器天然有序
 *   - 缺点：红黑树指针跳转多、CPU 缓存不友好；顶级撮合引擎会用"数组+位图(价格离散化)"或跳表优化
 *
 * ==================== 四、撮合规则（价格优先、时间优先）====================
 *  1. 新订单(taker/吃单)进来，先与对手方限价簿从最优价开始逐档撮合
 *  2. 成交价 = 对手方限价单(maker/挂单)的价格（保护吃单方，国际惯例）
 *  3. 限价单没撮合完 → 剩余部分再与对手方市价队列撮合 → 还不完 → 挂入本方订单簿等别人来吃
 *  4. 市价单没撮合完 → 剩余挂到本方市价队列等待（注意：这是有争议的设计，
 *     主流交易所市价单剩余部分会直接撤销，即 IOC(Immediate-Or-Cancel) 语义；
 *     本系统市价单会"滞留"队列，可能在未来某个价格成交，对用户有价格风险）
 *
 * ==================== 五、本方案的缺点（面试时主动讲，体现深度）====================
 *  1.【无高可用】撮合状态全在内存，单点故障后只能靠 CoinTraderEvent 从 DB 重建订单簿，
 *     重建期间该交易对停服；因此只能"按交易对拆分部署"：BTC/USDT 等热门对独占一个节点，
 *     冷门交易对多个合并部署到一个节点（用户已理解正确）
 *  2.【撤单与撮合的并发】cancelOrder 与 trade 来自两个 Kafka topic 的两个消费线程，
 *     靠 synchronized 保证队列结构安全；极端时序"撤单请求到达时订单刚被撮合完"，
 *     由下游 DB 状态机(ExchangeOrderService 里 status 校验)兜底，撤单会失败返回
 *  3.【资金最终一致】撮合在内存、清算在 market 模块异步落库，靠 Kafka at-least-once
 *     + DB 状态机幂等保证不丢不重（详见 ExchangeTradeConsumer 注释）
 *  4.【单交易对无法横向扩展】一个交易对的撮合是单线程，吞吐量有上限，
 *     只能通过交易对拆分扩容（水平分片思想）
 */
public class CoinTrader {
    private String symbol;
    private KafkaTemplate<String,String> kafkaTemplate;
    //交易币种的精度（如 BTC 保留4位小数），用于市价买单按成交额换算数量时的舍入
    private int coinScale = 4;
    //基币的精度（如 USDT 保留4位小数）
    private int baseCoinScale = 4;
    private Logger logger = LoggerFactory.getLogger(CoinTrader.class);
    //买入限价订单簿：TreeMap<价格, 同价订单合并列表>，价格从高到低排列（降序，firstKey=买一价）
    private TreeMap<BigDecimal,MergeOrder> buyLimitPriceQueue;
    //卖出限价订单簿：价格从低到高排列（升序，firstKey=卖一价）
    private TreeMap<BigDecimal,MergeOrder> sellLimitPriceQueue;
    //买入市价订单队列，按时间从小到大排序（市价单无价格，只排时间）
    private LinkedList<ExchangeOrder> buyMarketQueue;
    //卖出市价订单队列，按时间从小到大排序
    private LinkedList<ExchangeOrder> sellMarketQueue;
    //卖盘盘口信息（前端展示的卖1~卖100档，与订单簿冗余同步维护）
    private TradePlate sellTradePlate;
    //买盘盘口信息（前端展示的买1~买100档）
    private TradePlate buyTradePlate;
    //是否暂停交易
    private boolean tradingHalt = false;
    private boolean ready = false;
    //交易对信息
    private ExchangeCoinPublishType publishType;
    private String clearTime;
    
    private SimpleDateFormat dateTimeFormat;
    

    public CoinTrader(String symbol){
        this.symbol = symbol;
        initialize();
    }

    /**
     * 初始化交易线程（构造订单簿数据结构）
     * 【设计点】用 Comparator 控制两个方向的排序：
     *   买单簿降序 → 迭代器第一个就是"最高买价"（买一）
     *   卖单簿升序 → 迭代器第一个就是"最低卖价"（卖一）
     * 撮合时只需从头遍历，遇到"价格不满足成交条件"立即 break（见 matchLimitPriceWithLPList），
     * 这就是"价格优先"的落地：永远先成交最优价格档位。
     */
    public void initialize(){
        logger.info("init CoinTrader for symbol {}",symbol);
        //买单队列价格降序排列
        buyLimitPriceQueue = new TreeMap<>(Comparator.reverseOrder());
        //卖单队列价格升序排列
        this.sellLimitPriceQueue = new TreeMap<>(Comparator.naturalOrder());
        this.buyMarketQueue = new LinkedList<>();
        this.sellMarketQueue = new LinkedList<>();
        this.sellTradePlate = new TradePlate(symbol,ExchangeOrderDirection.SELL);
        this.buyTradePlate = new TradePlate(symbol,ExchangeOrderDirection.BUY);
        this.dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 增加限价订单到队列，买入单按从价格高到低排，卖出单按价格从低到高排
     * 【流程】① 先更新盘口 TradePlate 并立即推送盘口变化消息（前端实时看到新挂单）
     *        ② 再 synchronized 把订单挂到订单簿对应价格档位的 MergeOrder 尾部（时间优先：同价先到先排前面）
     * 【注意】ready 标志：撮合器启动重建订单簿(CoinTraderEvent)期间 ready=false，不发盘口消息，
     *        避免恢复过程中向前端推送大量中间状态的盘口（细节设计）
     * 【潜在问题】盘口更新(①)在锁外、订单簿更新(②)在锁内，两步非原子：
     *        极端并发下可能出现"盘口已推送但订单簿还没挂上"的短暂不一致，但盘口只是展示数据，不影响撮合正确性
     * @param exchangeOrder
     */
    public void addLimitPriceOrder(ExchangeOrder exchangeOrder){
        if(exchangeOrder.getType() != ExchangeOrderType.LIMIT_PRICE){
            return ;
        }
        //logger.info("addLimitPriceOrder,orderId = {}", exchangeOrder.getOrderId());
        TreeMap<BigDecimal,MergeOrder> list;
        if(exchangeOrder.getDirection() == ExchangeOrderDirection.BUY){
            list = buyLimitPriceQueue;
            buyTradePlate.add(exchangeOrder);
            if(ready) {
                sendTradePlateMessage(buyTradePlate);
            }
        } else {
            list = sellLimitPriceQueue;
            sellTradePlate.add(exchangeOrder);
            if(ready) {
                sendTradePlateMessage(sellTradePlate);
            }
        }
        //细粒度锁：只锁当前方向的订单簿，不影响对手方队列的并发操作
        synchronized (list) {
            MergeOrder mergeOrder = list.get(exchangeOrder.getPrice());
            if(mergeOrder == null){
                //该价格档位还没有订单，新建一个档位
                mergeOrder = new MergeOrder();
                mergeOrder.add(exchangeOrder);
                list.put(exchangeOrder.getPrice(),mergeOrder);
            }
            else {
                //同价位订单追加到档位尾部 → 同价格按时间先后排队（时间优先）
                mergeOrder.add(exchangeOrder);
            }
        }
    }

    public void addMarketPriceOrder(ExchangeOrder exchangeOrder){
        if(exchangeOrder.getType() != ExchangeOrderType.MARKET_PRICE){
            return ;
        }
        logger.info("addMarketPriceOrder,orderId = {}", exchangeOrder.getOrderId());
        LinkedList<ExchangeOrder> list = exchangeOrder.getDirection() == ExchangeOrderDirection.BUY ? buyMarketQueue : sellMarketQueue;
        synchronized (list) {
            list.addLast(exchangeOrder);
        }
    }

    public void trade(List<ExchangeOrder> orders) throws ParseException{
        if(tradingHalt) {
            return ;
        }
        for(ExchangeOrder order:orders){
            trade(order);
        }
    }


    /**
     * 【撮合入口】主动交易输入的订单，交易不完成的会输入到队列
     * 【撮合顺序（面试常考"撮合算法流程"）】
     *   市价单：只与对手方【限价簿】撮合（市价单之间不直接撮合，因为双方都没价格，无法定价）
     *   限价单：① 先与对手方【限价簿】撮合（价格优先+时间优先）
     *          ② 剩余部分再与对手方【市价队列】撮合（成交价用本方限价单的价格）
     *          ③ 仍有剩余 → 挂入本方限价簿，成为 maker 等待被吃
     * 【为什么市价单之间不能直接撮合？】两个市价单都没有价格，无法确定成交价。
     *   业界做法：要么禁止（币安现货市价单剩余直接撤销），要么以"最新成交价/对手方限价"定价（本系统用后者）
     * @param exchangeOrder
     * @throws ParseException
     */
    public void trade(ExchangeOrder exchangeOrder) throws ParseException{
        if(tradingHalt) {
            return ;
        }
        //logger.info("trade order={}",exchangeOrder);
        //防御：订单交易对必须与本撮合器一致（Kafka 消费端按 symbol 路由到对应 trader）
        if(!symbol.equalsIgnoreCase(exchangeOrder.getSymbol())){
            logger.info("unsupported symbol,coin={},base={}", exchangeOrder.getCoinSymbol(), exchangeOrder.getBaseSymbol());
            return ;
        }
        //防御：数量必须>0 且 剩余未成交量>0（启动恢复时已完成订单会被过滤掉，这里是双保险）
        if(exchangeOrder.getAmount().compareTo(BigDecimal.ZERO) <=0 || exchangeOrder.getAmount().subtract(exchangeOrder.getTradedAmount()).compareTo(BigDecimal.ZERO)<=0){
            return ;
        }

        //确定对手方订单簿：买单吃卖盘，卖单吃买盘
        TreeMap<BigDecimal,MergeOrder> limitPriceOrderList;
        LinkedList<ExchangeOrder> marketPriceOrderList;
        if(exchangeOrder.getDirection() == ExchangeOrderDirection.BUY){
            limitPriceOrderList = sellLimitPriceQueue;
            marketPriceOrderList = sellMarketQueue;
        }
        else{
            limitPriceOrderList = buyLimitPriceQueue;
            marketPriceOrderList = buyMarketQueue;
        }
        if(exchangeOrder.getType() == ExchangeOrderType.MARKET_PRICE){
            //logger.info(">>>>>市价单>>>交易与限价单交易");
            //市价单只与限价单交易（成交价取限价单价格）
            matchMarketPriceWithLPList(limitPriceOrderList, exchangeOrder);
        }else if(exchangeOrder.getType() == ExchangeOrderType.LIMIT_PRICE){
            //限价单价格必须大于0
            if(exchangeOrder.getPrice().compareTo(BigDecimal.ZERO) <= 0){
                return ;
            }
            // 抢购无需特殊处理，直接成交即可，但无法市价成交，在exchange-api做过滤控制
            // 仅分摊模式需要做特殊处理(分摊模式下，先有客户下一堆买单，然后由管理员发布卖单，根据比例分配)
            if(publishType == ExchangeCoinPublishType.FENTAN
            		&& exchangeOrder.getDirection() == ExchangeOrderDirection.SELL) {
            	logger.info(">>>>>分摊卖单>>>开始处理");
            	// 仅处在结束时间与清盘时间内的卖单需要特殊处理
				if(exchangeOrder.getTime().longValue() < dateTimeFormat.parse(clearTime).getTime()) {
					logger.info(">>>>>分摊卖单>>>处在结束时间与清盘时间内");
					//将卖单分摊成交，活动卖单无需压入卖盘显示在前端
					matchLimitPriceWithLPListByFENTAN(limitPriceOrderList, exchangeOrder, false);
					return;
				}
            }
            //logger.info(">>>>>限价单>>>交易与限价单交易");
            //先与限价单交易
            matchLimitPriceWithLPList(limitPriceOrderList, exchangeOrder,false);
            if(exchangeOrder.getAmount().compareTo(exchangeOrder.getTradedAmount()) > 0) {
                //logger.info(">>>>限价单未交易完>>>>与市价单交易>>>>");
                //后与市价单交易
                matchLimitPriceWithMPList(marketPriceOrderList, exchangeOrder);
            }
        }
    }
    /**
     * 分摊抢购模式下的交易处理规则
     * @param lpList
     * @param focusedOrder
     * @param canEnterList
     */
    
    public void matchLimitPriceWithLPListByFENTAN(TreeMap<BigDecimal,MergeOrder> lpList, ExchangeOrder focusedOrder,boolean canEnterList) {
    	List<ExchangeTrade> exchangeTrades = new ArrayList<>();
        List<ExchangeOrder> completedOrders = new ArrayList<>();
        synchronized (lpList) {
            Iterator<Map.Entry<BigDecimal,MergeOrder>> mergeOrderIterator = lpList.entrySet().iterator();
            boolean exitLoop = false;
            while (!exitLoop && mergeOrderIterator.hasNext()) {
                Map.Entry<BigDecimal,MergeOrder> entry = mergeOrderIterator.next();
                MergeOrder mergeOrder = entry.getValue();
                Iterator<ExchangeOrder> orderIterator = mergeOrder.iterator();
                //买入单需要匹配的价格不大于委托价，否则退出
                if (focusedOrder.getDirection() == ExchangeOrderDirection.BUY && mergeOrder.getPrice().compareTo(focusedOrder.getPrice()) > 0) {
                    break;
                }
                //卖出单需要匹配的价格不小于委托价，否则退出
                if (focusedOrder.getDirection() == ExchangeOrderDirection.SELL && mergeOrder.getPrice().compareTo(focusedOrder.getPrice()) < 0) {
                    break;
                }
                BigDecimal totalAmount = mergeOrder.getTotalAmount();
                while (orderIterator.hasNext()) {
                    ExchangeOrder matchOrder = orderIterator.next();
                    //处理匹配
                    ExchangeTrade trade = processMatchByFENTAN(focusedOrder, matchOrder, totalAmount);
                    exchangeTrades.add(trade);
                    //判断匹配单是否完成
                    if (matchOrder.isCompleted()) {
                        //当前匹配的订单完成交易，删除该订单
                        orderIterator.remove();
                        completedOrders.add(matchOrder);
                    }
                    //判断交易单是否完成
                    if (focusedOrder.isCompleted()) {
                        //交易完成
                        completedOrders.add(focusedOrder);
                        //退出循环
                        exitLoop = true;
                        break;
                    }
                }
                if(mergeOrder.size() == 0){
                    mergeOrderIterator.remove();
                }
            }
        }
        //如果还没有交易完，订单压入列表中
        if (focusedOrder.getTradedAmount().compareTo(focusedOrder.getAmount()) < 0 && canEnterList) {
            addLimitPriceOrder(focusedOrder);
        }
        //每个订单的匹配批量推送
        handleExchangeTrade(exchangeTrades);
        if(completedOrders.size() > 0){
            orderCompleted(completedOrders);
            TradePlate plate = focusedOrder.getDirection() == ExchangeOrderDirection.BUY ? sellTradePlate : buyTradePlate;
            sendTradePlateMessage(plate);
        }
    }
    /**
     * 【核心撮合算法】限价委托单与限价队列匹配（价格优先 + 时间优先的完整实现）
     *
     * 【算法流程】
     *   外层循环：从对手方订单簿【最优价档位】开始逐档扫描（TreeMap 有序，天然价格优先）
     *     ├─ 买单：对手档价格 > 我的买价 → 太贵了，break（后面的档位更贵，直接结束）
     *     └─ 卖单：对手档价格 < 我的卖价 → 太便宜了，break
     *   内层循环：同一价格档位内按订单到达顺序逐个撮合（MergeOrder 是 ArrayList，时间优先/FIFO）
     *     ├─ processMatch() 成交一笔：成交量 = min(双方剩余量)，成交价 = 对手方(maker)价格
     *     ├─ 对手单(maker)完全成交 → 从档位中移除，加入完成列表
     *     └─ 本方单(taker)完全成交 → 结束整个撮合
     *   档位被吃空 → 从订单簿删除该价格档（mergeOrderIterator.remove()）
     *
     * 【锁的学问】synchronized(lpList) 锁住整个撮合过程：
     *   - 保证"遍历+成交+移除"是原子的，撤单线程此时无法插进来把正在撮合的订单撤掉
     *   - 临界区内全是内存操作（BigDecimal 运算），无 IO，锁持有时间极短（微秒级）
     *   - 这就是"无锁高并发"的真相：主流程单线程，锁只是防御撤单线程，几乎无竞争
     *
     * 【异步通知】撮合完成后才发 Kafka（在锁外发送，避免 IO 阻塞撮合锁）：
     *   - handleExchangeTrade: 成交明细 → 清算/行情
     *   - orderCompleted: 完成订单 → 更新DB状态+退冻结
     *   - sendTradePlateMessage: 盘口变化 → 前端刷新
     *   注意：先发消息再入库是"先内存后落库"，宕机时靠 Kafka 重放恢复（事件溯源思想的简化版）
     *
     * @param lpList 限价对手单队列
     * @param focusedOrder 交易订单（焦点单/taker）
     * @param canEnterList 未撮合完是否允许挂入订单簿（分摊模式下的活动卖单不允许挂簿）
     */
    public void matchLimitPriceWithLPList(TreeMap<BigDecimal,MergeOrder> lpList, ExchangeOrder focusedOrder,boolean canEnterList){
        List<ExchangeTrade> exchangeTrades = new ArrayList<>();
        List<ExchangeOrder> completedOrders = new ArrayList<>();
        synchronized (lpList) {
            Iterator<Map.Entry<BigDecimal,MergeOrder>> mergeOrderIterator = lpList.entrySet().iterator();
            boolean exitLoop = false;
            while (!exitLoop && mergeOrderIterator.hasNext()) {
                Map.Entry<BigDecimal,MergeOrder> entry = mergeOrderIterator.next();
                MergeOrder mergeOrder = entry.getValue();
                Iterator<ExchangeOrder> orderIterator = mergeOrder.iterator();

                //【价格优先的截断点】买入单需要匹配的价格不大于委托价，否则退出
                //（买单簿降序/卖单簿升序遍历，一旦当前档位不满足价格条件，后续档位必然更不满足，直接 break）
                if (focusedOrder.getDirection() == ExchangeOrderDirection.BUY && mergeOrder.getPrice().compareTo(focusedOrder.getPrice()) > 0) {
                    break;
                }
                //卖出单需要匹配的价格不小于委托价，否则退出
                if (focusedOrder.getDirection() == ExchangeOrderDirection.SELL && mergeOrder.getPrice().compareTo(focusedOrder.getPrice()) < 0) {
                    break;
                }
                while (orderIterator.hasNext()) {
                    ExchangeOrder matchOrder = orderIterator.next();

                    //处理匹配（核心成交逻辑，见 processMatch）
                    ExchangeTrade trade = processMatch(focusedOrder, matchOrder);
                    exchangeTrades.add(trade);
                    //判断匹配单(maker)是否完成
                    if (matchOrder.isCompleted()) {
                        //当前匹配的订单完成交易，删除该订单
                        orderIterator.remove();
                        completedOrders.add(matchOrder);
                    }
                    //判断交易单(taker)是否完成
                    if (focusedOrder.isCompleted()) {
                        //交易完成
                        completedOrders.add(focusedOrder);
                        //退出循环
                        exitLoop = true;
                        break;
                    }
                }
                //该价格档位订单全部成交完毕，从订单簿移除空档位（保持订单簿精简，避免空档累积）
                if(mergeOrder.size() == 0){
                    mergeOrderIterator.remove();
                }
            }
        }
        //如果还没有交易完，订单压入列表中（taker 变 maker，挂到本方订单簿等待被吃）
        if (focusedOrder.getTradedAmount().compareTo(focusedOrder.getAmount()) < 0 && canEnterList) {
            addLimitPriceOrder(focusedOrder);
        }
        //每个订单的匹配批量推送（注意：在 synchronized 锁外发 Kafka，IO 不占用撮合锁，关键性能细节）
        handleExchangeTrade(exchangeTrades);
        if(completedOrders.size() > 0){
            orderCompleted(completedOrders);
            TradePlate plate = focusedOrder.getDirection() == ExchangeOrderDirection.BUY ? sellTradePlate : buyTradePlate;
            sendTradePlateMessage(plate);
        }
    }

    /**
     * 限价委托单与市价队列匹配
     * @param mpList 市价对手单队列
     * @param focusedOrder 交易订单
     */
    public void matchLimitPriceWithMPList(LinkedList<ExchangeOrder> mpList,ExchangeOrder focusedOrder){
        List<ExchangeTrade> exchangeTrades = new ArrayList<>();
        List<ExchangeOrder> completedOrders = new ArrayList<>();
        synchronized (mpList) {
            Iterator<ExchangeOrder> iterator = mpList.iterator();
            while (iterator.hasNext()) {
                ExchangeOrder matchOrder = iterator.next();
                ExchangeTrade trade = processMatch(focusedOrder, matchOrder);
                logger.info(">>>>>"+trade);
                if(trade != null){
                    exchangeTrades.add(trade);
                }
                //判断匹配单是否完成，市价单amount为成交量
                if(matchOrder.isCompleted()){
                    iterator.remove();
                    completedOrders.add(matchOrder);
                }
                //判断吃单是否完成，判断成交量是否完成
                if (focusedOrder.isCompleted()) {
                    //交易完成
                    completedOrders.add(focusedOrder);
                    //退出循环
                    break;
                }
            }
        }
        //如果还没有交易完，订单压入列表中
        if (focusedOrder.getTradedAmount().compareTo(focusedOrder.getAmount()) < 0) {
            addLimitPriceOrder(focusedOrder);
        }
        //每个订单的匹配批量推送
        handleExchangeTrade(exchangeTrades);
        orderCompleted(completedOrders);
    }


    /**
     * 市价委托单与限价对手单列表交易
     * @param lpList  限价对手单列表
     * @param focusedOrder 待交易订单
     */
    public void matchMarketPriceWithLPList(TreeMap<BigDecimal,MergeOrder> lpList, ExchangeOrder focusedOrder){
        List<ExchangeTrade> exchangeTrades = new ArrayList<>();
        List<ExchangeOrder> completedOrders = new ArrayList<>();
        synchronized (lpList) {
            Iterator<Map.Entry<BigDecimal,MergeOrder>> mergeOrderIterator = lpList.entrySet().iterator();
            boolean exitLoop = false;
            while (!exitLoop && mergeOrderIterator.hasNext()) {
                Map.Entry<BigDecimal,MergeOrder> entry = mergeOrderIterator.next();
                MergeOrder mergeOrder = entry.getValue();
                Iterator<ExchangeOrder> orderIterator = mergeOrder.iterator();
                while (orderIterator.hasNext()) {
                    ExchangeOrder matchOrder = orderIterator.next();
                    //处理匹配
                    ExchangeTrade trade = processMatch(focusedOrder, matchOrder);
                    if (trade != null) {
                        exchangeTrades.add(trade);
                    }
                    //判断匹配单是否完成
                    if (matchOrder.isCompleted()) {
                        //当前匹配的订单完成交易，删除该订单
                        orderIterator.remove();
                        completedOrders.add(matchOrder);
                    }
                    //判断焦点订单是否完成
                    if (focusedOrder.isCompleted()) {
                        completedOrders.add(focusedOrder);
                        //退出循环
                        exitLoop = true;
                        break;
                    }
                }
                if(mergeOrder.size() == 0){
                    mergeOrderIterator.remove();
                }
            }
        }
        //如果还没有交易完，订单压入列表中,市价买单按成交量算
        if (focusedOrder.getDirection() == ExchangeOrderDirection.SELL&&focusedOrder.getTradedAmount().compareTo(focusedOrder.getAmount()) < 0
                || focusedOrder.getDirection() == ExchangeOrderDirection.BUY&& focusedOrder.getTurnover().compareTo(focusedOrder.getAmount()) < 0) {
            addMarketPriceOrder(focusedOrder);
        }
        //每个订单的匹配批量推送
        handleExchangeTrade(exchangeTrades);
        if(completedOrders.size() > 0){
            orderCompleted(completedOrders);
            TradePlate plate = focusedOrder.getDirection() == ExchangeOrderDirection.BUY ? sellTradePlate : buyTradePlate;
            sendTradePlateMessage(plate);
        }
    }

    /**
     * 计算委托单剩余可成交的数量
     * 【关键细节】市价买单的 amount 字段存的是"总成交额(USDT)"而不是币数量！
     *   所以市价买单的剩余可成交量 = 剩余成交额 / 当前成交价（ROUND_DOWN 向下取整，防止超花用户的钱）
     *   其他单（限价单/市价卖单）amount 就是币数量，剩余量 = amount - 已成交量
     * @param order 委托单
     * @param dealPrice 成交价
     * @return
     */
    private BigDecimal calculateTradedAmount(ExchangeOrder order, BigDecimal dealPrice){
        if(order.getDirection() == ExchangeOrderDirection.BUY && order.getType() == ExchangeOrderType.MARKET_PRICE){
            //剩余成交量 = 剩余成交额 / 成交价（向下取整到币种精度，宁可少买不能多扣）
            BigDecimal leftTurnover = order.getAmount().subtract(order.getTurnover());
            return leftTurnover.divide(dealPrice,coinScale,BigDecimal.ROUND_DOWN);
        }
        else{
            return  order.getAmount().subtract(order.getTradedAmount());
        }
    }

    /**
     * 调整市价单剩余成交额，当剩余成交额不足时设置订单完成
     * 【解决的问题】市价买单按"成交额"下单（如花 100 USDT 买 BTC），逐笔成交后可能剩下
     *   0.0001 USDT 这种"不够买最小精度币"的零头，如果不处理订单永远无法完成。
     *   这里把零头直接并进最后一笔成交额（turnover 设为 amount），让订单正常完结。
     *   —— 面试点：金额类系统必须处理"除不尽的零头"，常见做法就是归并到最后一笔（尾差处理）
     * @param order
     * @param dealPrice
     * @return
     */
    private BigDecimal adjustMarketOrderTurnover(ExchangeOrder order, BigDecimal dealPrice){
        if(order.getDirection() == ExchangeOrderDirection.BUY && order.getType() == ExchangeOrderType.MARKET_PRICE){
            BigDecimal leftTurnover = order.getAmount().subtract(order.getTurnover());
            if(leftTurnover.divide(dealPrice,coinScale,BigDecimal.ROUND_DOWN)
                    .compareTo(BigDecimal.ZERO)==0){
                //剩余成交额已经买不起最小精度的币了，把零头并入本次成交，订单标记完成
                order.setTurnover(order.getAmount());
                return leftTurnover;
            }
        }
        return BigDecimal.ZERO;
    }

    /**
     * 【成交核心】处理两个匹配的委托订单（一次撮合产生一条 ExchangeTrade）
     * 【定价规则】成交价 = maker(挂单/被吃单) 的价格：
     *   - 对手单是限价单 → 用对手单价格（taker 享受更优价，国际惯例，保护吃单方）
     *   - 对手单是市价单 → 用本方(限价单)价格（此时本方是限价单，对手市价单接受任何价格）
     * 【成交量】= min(双方剩余可成交量)，即"能成交多少成交多少"（部分成交是常态）
     * 【状态累积】直接修改内存中订单对象的 tradedAmount/turnover（不落库！），
     *   订单完全成交后才通过 Kafka 异步回写 DB —— 这是内存撮合高性能的关键：
     *   撮合路径上零数据库操作，DB 写入被异步化、批量化
     * @param focusedOrder 焦点单(taker/吃单方)
     * @param matchOrder 匹配单(maker/挂单方)
     * @return
     */
    private ExchangeTrade processMatch(ExchangeOrder focusedOrder, ExchangeOrder matchOrder){
        //需要交易的数量，成交量,成交价，可用数量
        BigDecimal needAmount,dealPrice,availAmount;
        //如果匹配单是限价单，则以其价格为成交价（maker 定价原则）
        if(matchOrder.getType() == ExchangeOrderType.LIMIT_PRICE){
            dealPrice = matchOrder.getPrice();
        }
        else {
            dealPrice = focusedOrder.getPrice();
        }
        //成交价必须大于0（防御：市价单对市价单时双方价格都为0，无法定价，跳过）
        if(dealPrice.compareTo(BigDecimal.ZERO) <= 0){
            return null;
        }
        needAmount = calculateTradedAmount(focusedOrder,dealPrice);
        availAmount = calculateTradedAmount(matchOrder,dealPrice);
        //计算成交量 = min(我需要买的, 对手能卖的)
        BigDecimal tradedAmount = (availAmount.compareTo(needAmount) >= 0 ? needAmount : availAmount);
        logger.info("dealPrice={},amount={}",dealPrice,tradedAmount);
        //如果成交额为0说明剩余额度无法成交，退出（如市价买单剩余钱不够买最小单位）
        if(tradedAmount.compareTo(BigDecimal.ZERO) == 0){
            return null;
        }

        //计算成交额,成交额要保留足够精度
        BigDecimal turnover = tradedAmount.multiply(dealPrice);
        //【内存状态累积】双方订单的已成交量/已成交额直接改内存对象，全程无 DB 操作
        matchOrder.setTradedAmount(matchOrder.getTradedAmount().add(tradedAmount));
        matchOrder.setTurnover(matchOrder.getTurnover().add(turnover));
        focusedOrder.setTradedAmount(focusedOrder.getTradedAmount().add(tradedAmount));
        focusedOrder.setTurnover(focusedOrder.getTurnover().add(turnover));

        //创建成交记录
        ExchangeTrade exchangeTrade = new ExchangeTrade();
        exchangeTrade.setSymbol(symbol);
        exchangeTrade.setAmount(tradedAmount);
        exchangeTrade.setDirection(focusedOrder.getDirection());
        exchangeTrade.setPrice(dealPrice);
        exchangeTrade.setBuyTurnover(turnover);
        exchangeTrade.setSellTurnover(turnover);
        //校正市价单剩余成交额
        if(ExchangeOrderType.MARKET_PRICE == focusedOrder.getType() && focusedOrder.getDirection() == ExchangeOrderDirection.BUY){
            BigDecimal adjustTurnover = adjustMarketOrderTurnover(focusedOrder,dealPrice);
            exchangeTrade.setBuyTurnover(turnover.add(adjustTurnover));
        }
        else if(ExchangeOrderType.MARKET_PRICE == matchOrder.getType() && matchOrder.getDirection() == ExchangeOrderDirection.BUY){
            BigDecimal adjustTurnover = adjustMarketOrderTurnover(matchOrder,dealPrice);
            exchangeTrade.setBuyTurnover(turnover.add(adjustTurnover));
        }

        if (focusedOrder.getDirection() == ExchangeOrderDirection.BUY) {
            exchangeTrade.setBuyOrderId(focusedOrder.getOrderId());
            exchangeTrade.setSellOrderId(matchOrder.getOrderId());
        } else {
            exchangeTrade.setBuyOrderId(matchOrder.getOrderId());
            exchangeTrade.setSellOrderId(focusedOrder.getOrderId());
        }

        exchangeTrade.setTime(Calendar.getInstance().getTimeInMillis());
        if(matchOrder.getType() == ExchangeOrderType.LIMIT_PRICE){
            if(matchOrder.getDirection() == ExchangeOrderDirection.BUY){
                buyTradePlate.remove(matchOrder,tradedAmount);
            }
            else{
                sellTradePlate.remove(matchOrder,tradedAmount);
            }
        }
        return  exchangeTrade;
    }

    /**
     * 处理两个匹配的委托订单
     * @param focusedOrder 焦点单
     * @param matchOrder 匹配单
     * @return
     */
    private ExchangeTrade processMatchByFENTAN(ExchangeOrder focusedOrder, ExchangeOrder matchOrder, BigDecimal totalAmount){
        //需要交易的数量，成交量,成交价，可用数量
        BigDecimal dealPrice;
        //如果匹配单是限价单，则以其价格为成交价
        if(matchOrder.getType() == ExchangeOrderType.LIMIT_PRICE){
            dealPrice = matchOrder.getPrice();
        }
        else {
            dealPrice = focusedOrder.getPrice();
        }
        //成交价必须大于0
        if(dealPrice.compareTo(BigDecimal.ZERO) <= 0){
            return null;
        }
        // 成交数 = 发行卖单总数*匹配单数量占比（例：1.2345%）
        //计算成交量
        BigDecimal tradedAmount = focusedOrder.getAmount().multiply(matchOrder.getAmount().divide(totalAmount, 8, BigDecimal.ROUND_HALF_DOWN)).setScale(8, BigDecimal.ROUND_HALF_DOWN);
        logger.info("dealPrice={},amount={}",dealPrice,tradedAmount);
        //如果成交额为0说明剩余额度无法成交，退出
        if(tradedAmount.compareTo(BigDecimal.ZERO) == 0){
            return null;
        }

        //计算成交额,成交额要保留足够精度
        BigDecimal turnover = tradedAmount.multiply(dealPrice).setScale(8, BigDecimal.ROUND_HALF_DOWN);
        matchOrder.setTradedAmount(matchOrder.getTradedAmount().add(tradedAmount).setScale(8, BigDecimal.ROUND_HALF_DOWN));
        matchOrder.setTurnover(matchOrder.getTurnover().add(turnover).setScale(8, BigDecimal.ROUND_HALF_DOWN));
        focusedOrder.setTradedAmount(focusedOrder.getTradedAmount().add(tradedAmount).setScale(8, BigDecimal.ROUND_HALF_DOWN));
        focusedOrder.setTurnover(focusedOrder.getTurnover().add(turnover).setScale(8, BigDecimal.ROUND_HALF_DOWN));

        //创建成交记录
        ExchangeTrade exchangeTrade = new ExchangeTrade();
        exchangeTrade.setSymbol(symbol);
        exchangeTrade.setAmount(tradedAmount);
        exchangeTrade.setDirection(focusedOrder.getDirection());
        exchangeTrade.setPrice(dealPrice);
        exchangeTrade.setBuyTurnover(turnover);
        exchangeTrade.setSellTurnover(turnover);

        if (focusedOrder.getDirection() == ExchangeOrderDirection.BUY) {
            exchangeTrade.setBuyOrderId(focusedOrder.getOrderId());
            exchangeTrade.setSellOrderId(matchOrder.getOrderId());
        } else {
            exchangeTrade.setBuyOrderId(matchOrder.getOrderId());
            exchangeTrade.setSellOrderId(focusedOrder.getOrderId());
        }

        exchangeTrade.setTime(Calendar.getInstance().getTimeInMillis());
        if(matchOrder.getType() == ExchangeOrderType.LIMIT_PRICE){
            if(matchOrder.getDirection() == ExchangeOrderDirection.BUY){
                buyTradePlate.remove(matchOrder,tradedAmount);
            }
            else{
                sellTradePlate.remove(matchOrder,tradedAmount);
            }
        }
        return  exchangeTrade;
    }
    
    /**
     * 发送成交明细到 Kafka（topic: exchange-trade）
     * 【Kafka 顺序性·面试重点】
     *   - 这里 send 没有指定 key！Kafka 默认分区策略下，无 key 消息会"轮询/粘性"分配到多个分区，
     *     如果 exchange-trade 是多分区 topic，同一交易对的成交明细可能被分到不同分区，
     *     消费端多线程并发消费时【无法保证成交的全局顺序】。
     *   - 正确做法：kafkaTemplate.send("exchange-trade", symbol, json)，以 symbol 为 key，
     *     保证同一交易对的所有消息进同一分区 → 分区内有顺序 → 消费者按序处理。
     *     （注释：本系统 exchange-trade 下游是 market 模块的线程池并发处理，本身就没强依赖顺序，
     *       但 K线聚合/订单回写乱序会有小概率数据毛刺，这是架构上的一个妥协点）
     * 【批量拆分】单条 Kafka 消息默认上限约 1MB，一次撮合可能产生上千条成交（大单吃深盘口），
     *   按 1000 条/批拆分发送，防止消息体超限被拒。
     * 【性能】kafkaTemplate.send 是异步的（消息先进 producer 缓冲区，后台线程批量发送），
     *   不阻塞撮合主流程 —— 撮合与 IO 解耦。
     */
    public void handleExchangeTrade(List<ExchangeTrade> trades){
        //logger.info("handleExchangeTrade:{}", trades);
        if(trades.size() > 0) {
            int maxSize = 1000;
            //发送消息，key为交易对符号（注释：实际代码并未传 key，见方法头注释的顺序性分析）
            if(trades.size() > maxSize) {
                int size = trades.size();
                for(int index = 0;index < size;index += maxSize){
                    int length = (size - index) > maxSize ? maxSize : size - index;
                    List<ExchangeTrade> subTrades = trades.subList(index,index + length);
                    kafkaTemplate.send("exchange-trade",JSON.toJSONString(subTrades));
                }
            }
            else {
                kafkaTemplate.send("exchange-trade",JSON.toJSONString(trades));
            }
        }
    }

    /**
     * 订单完成，执行消息通知,订单数超1000个要拆分发送
     * 【下游】market 模块 ExchangeTradeConsumer.handleOrderCompleted 消费：
     *   更新 DB 订单状态(TRADING→COMPLETED) + 退回剩余冻结资金 + WebSocket/Netty 通知用户
     * 【幂等性】下游 tradeCompleted() 有状态机校验（只有 TRADING 状态才处理），
     *   Kafka 重复投递时第二次会因状态已是 COMPLETED 而拒绝 → 天然幂等（状态机幂等模式）
     * @param orders
     */
    public  void orderCompleted(List<ExchangeOrder> orders){
        //logger.info("orderCompleted ,order={}",orders);
        if(orders.size() > 0) {
            int maxSize = 1000;
            if(orders.size() > maxSize){
                int size = orders.size();
                for(int index = 0;index < size;index += maxSize){
                    int length = (size - index) > maxSize ? maxSize : size - index;
                    List<ExchangeOrder> subOrders = orders.subList(index,index + length);
                    kafkaTemplate.send("exchange-order-completed", JSON.toJSONString(subOrders));
                }
            }
            else {
                kafkaTemplate.send("exchange-order-completed", JSON.toJSONString(orders));
            }
        }
    }

    /**
     * 发送盘口变化消息
     * 【为什么这里要 synchronized(plate)？注释里写得很明白：防止并发引起数组越界、盘口倒挂】
     *   JSON.toJSONString(plate) 会遍历 plate.items 这个 LinkedList，
     *   如果此刻另一个线程（如撤单）正在 add/remove items，LinkedList 迭代会抛
     *   ConcurrentModificationException 或序列化出"买一价 > 卖一价"的倒挂脏数据。
     *   序列化（读）与盘口增删（写）用同一把 plate 锁互斥。
     * 【缺点】盘口全量推送（100档），高频交易对网络开销大；业界做法是推"增量 diff"（如币安的 depth stream）
     * @param plate
     */
    public void sendTradePlateMessage(TradePlate plate){
        //防止并发引起数组越界，造成盘口倒挂
        synchronized (plate) {
            kafkaTemplate.send("exchange-trade-plate", JSON.toJSONString(plate));
        }
    }

    /**
     * 取消委托订单（由 Kafka topic=exchange-order-cancel 的消费线程调用）
     * 【与撮合的并发关系·面试重点】
     *   撤单线程与撮合线程是两个不同的 Kafka 消费线程，可能并发：
     *   - 两者都通过 synchronized(同一个list) 互斥 → 队列结构不会损坏
     *   - 时序竞争：若撮合先完成，订单已从簿中移除，这里撤单会返回 null（找不到订单），
     *     消费端就不发 cancel-success；若撤单先执行，订单被移除，后续撮合自然吃不到它
     *   - 极端情况"撤单消息与成交消息几乎同时发出"：下游 DB 层用状态机兜底
     *     （cancelOrder/tradeCompleted 都校验 status==TRADING，先到者赢，后到者失败）
     *   → 这就是"无锁无事务下保证数据安全"的完整链路：内存靠单线程+细粒度锁，
     *     跨服务靠 Kafka 顺序投递，最终一致性靠 DB 状态机幂等
     * @param exchangeOrder
     * @return
     */
    public ExchangeOrder cancelOrder(ExchangeOrder exchangeOrder){
        logger.info("cancelOrder,orderId={}", exchangeOrder.getOrderId());
        if(exchangeOrder.getType() == ExchangeOrderType.MARKET_PRICE){
            //处理市价单
            Iterator<ExchangeOrder> orderIterator;
            List<ExchangeOrder> list = null;
            if(exchangeOrder.getDirection() == ExchangeOrderDirection.BUY){
                list = this.buyMarketQueue;
            } else{
                list = this.sellMarketQueue;
            }
            synchronized (list) {
                orderIterator = list.iterator();
                while ((orderIterator.hasNext())) {
                    ExchangeOrder order = orderIterator.next();
                    if (order.getOrderId().equalsIgnoreCase(exchangeOrder.getOrderId())) {
                        orderIterator.remove();
                        onRemoveOrder(order);
                        return order;
                    }
                }
            }
        } else {
            //处理限价单
            TreeMap<BigDecimal,MergeOrder> list = null;
            Iterator<MergeOrder> mergeOrderIterator;
            if(exchangeOrder.getDirection() == ExchangeOrderDirection.BUY){
                list = this.buyLimitPriceQueue;
            } else{
                list = this.sellLimitPriceQueue;
            }
            synchronized (list) {
                MergeOrder mergeOrder = list.get(exchangeOrder.getPrice());
                if(mergeOrder!=null) {
                    Iterator<ExchangeOrder> orderIterator = mergeOrder.iterator();
                    while (orderIterator.hasNext()) {
                        ExchangeOrder order = orderIterator.next();
                        if (order.getOrderId().equalsIgnoreCase(exchangeOrder.getOrderId())) {
                            orderIterator.remove();
                            if (mergeOrder.size() == 0) {
                                list.remove(exchangeOrder.getPrice());
                            }
                            onRemoveOrder(order);
                            return order;
                        }
                    }
                }
            }
        }
        return  null;
    }

    public void onRemoveOrder(ExchangeOrder order){
        if (order.getType() == ExchangeOrderType.LIMIT_PRICE) {
            if (order.getDirection() == ExchangeOrderDirection.BUY) {
                buyTradePlate.remove(order);
                sendTradePlateMessage(buyTradePlate);
            } else {
                sellTradePlate.remove(order);
                sendTradePlateMessage(sellTradePlate);
            }
        }
    }



    public TradePlate getTradePlate(ExchangeOrderDirection direction){
        if(direction == ExchangeOrderDirection.BUY){
            return buyTradePlate;
        }
        else{
            return sellTradePlate;
        }
    }



    /**
     * 查询交易器里的订单
     * @param orderId
     * @param type
     * @param direction
     * @return
     */
    public ExchangeOrder findOrder(String orderId,ExchangeOrderType type,ExchangeOrderDirection direction){
        if(type == ExchangeOrderType.MARKET_PRICE){
            LinkedList<ExchangeOrder> list;
            if(direction == ExchangeOrderDirection.BUY){
                list = this.buyMarketQueue;
            } else{
                list = this.sellMarketQueue;
            }
            synchronized (list) {
                Iterator<ExchangeOrder> orderIterator = list.iterator();
                while ((orderIterator.hasNext())) {
                    ExchangeOrder order = orderIterator.next();
                    if (order.getOrderId().equalsIgnoreCase(orderId)) {
                        return order;
                    }
                }
            }
        } else {
            TreeMap<BigDecimal,MergeOrder> list;
            if(direction == ExchangeOrderDirection.BUY){
                list = this.buyLimitPriceQueue;
            } else{
                list = this.sellLimitPriceQueue;
            }
            synchronized (list) {
                Iterator<Map.Entry<BigDecimal,MergeOrder>> mergeOrderIterator = list.entrySet().iterator();
                while (mergeOrderIterator.hasNext()) {
                    Map.Entry<BigDecimal,MergeOrder> entry = mergeOrderIterator.next();
                    MergeOrder mergeOrder = entry.getValue();
                    Iterator<ExchangeOrder> orderIterator = mergeOrder.iterator();
                    while ((orderIterator.hasNext())) {
                        ExchangeOrder order = orderIterator.next();
                        if (order.getOrderId().equalsIgnoreCase(orderId)) {
                            return order;
                        }
                    }
                }
            }
        }
        return null;
    }

    public TreeMap<BigDecimal,MergeOrder> getBuyLimitPriceQueue() {
        return buyLimitPriceQueue;
    }

    public LinkedList<ExchangeOrder> getBuyMarketQueue() {
        return buyMarketQueue;
    }

    public TreeMap<BigDecimal,MergeOrder> getSellLimitPriceQueue() {
        return sellLimitPriceQueue;
    }

    public LinkedList<ExchangeOrder> getSellMarketQueue() {
        return sellMarketQueue;
    }

    public void setKafkaTemplate(KafkaTemplate<String,String> template){
        this.kafkaTemplate = template;
    }
    public void setCoinScale(int scale){
        this.coinScale = scale;
    }

    public void setBaseCoinScale(int scale){
        this.baseCoinScale = scale;
    }

    public boolean isTradingHalt(){
        return this.tradingHalt;
    }

    /**
     * 暂停交易,不接收新的订单
     * 【注意】tradingHalt/ready 是普通 boolean，非 volatile：
     *   严格说存在可见性问题（管理线程设置后，撮合线程可能短时间看不到），
     *   但因为这些标志由 Kafka 消费线程在撮合前后检查，且 Kafka 消费有内存屏障语义，
     *   实际风险极低。更严谨的写法应加 volatile —— 面试可主动指出这个改进点。
     */
    public void haltTrading(){
        this.tradingHalt = true;
    }

    /**
     * 恢复交易
     */
    public void resumeTrading(){
        this.tradingHalt = false;
    }

    public void stopTrading(){
        //TODO:停止交易，取消当前所有订单
    }

    public boolean getReady(){
        return this.ready;
    }

    public void setReady(boolean ready){
        this.ready = ready;
    }
    public void setPublishType(ExchangeCoinPublishType publishType) {
    	this.publishType = publishType;
    }
    public void setClearTime(String clearTime) {
    	this.clearTime = clearTime;
    }
    public int getLimitPriceOrderCount(ExchangeOrderDirection direction){
        int count = 0;
        TreeMap<BigDecimal,MergeOrder> queue = direction == ExchangeOrderDirection.BUY ? buyLimitPriceQueue : sellLimitPriceQueue;
        Iterator<Map.Entry<BigDecimal,MergeOrder>> mergeOrderIterator = queue.entrySet().iterator();
        while (mergeOrderIterator.hasNext()) {
            Map.Entry<BigDecimal,MergeOrder> entry = mergeOrderIterator.next();
            MergeOrder mergeOrder = entry.getValue();
            count += mergeOrder.size();
        }
        return count;
    }
}
