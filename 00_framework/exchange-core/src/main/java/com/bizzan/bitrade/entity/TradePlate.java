package com.bizzan.bitrade.entity;


import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.LinkedList;

/**
 * 【盘口·面试必问"盘口/深度图怎么实现"】
 *
 * 【本质】盘口 = 订单簿的"聚合视图"：把同一价格的所有订单量加总，按价格排序展示。
 *   买单盘：价格从高到低（买1是最高买价）；卖单盘：价格从低到高（卖1是最低卖价）
 *
 * 【本实现的设计：冗余存储、同步维护】
 *   撮合引擎里订单簿(TreeMap<价格,MergeOrder>)是"交易用的"，本类是"展示用的"，两份数据：
 *   - 挂单时 add()：找到价格档位累加数量（或插入新档位，保持有序）
 *   - 成交/撤单时 remove()：扣减档位数量，扣到 0 删除档位
 *   好处：推送盘口时直接序列化 items，不用临时遍历整个订单簿聚合（用内存换 CPU）
 *   代价：两份数据要保持一致，任何遗漏都会导致"盘口与真实订单簿不符"
 *
 * 【数据结构选择】items 用 LinkedList + 线性查找插入：
 *   - 档位最多 maxDepth=100，线性扫描 O(100) 常数很小，比 TreeMap 简单
 *   - 缺点：每次 add/remove 都是 O(N) 遍历；档位多时不如 TreeMap O(logN)
 *     （但注意：盘口只保留前100档，超出深度的挂单不进盘口，所以 N 有上界）
 *
 * 【线程安全】add/remove 都 synchronized(items)，与撮合主流程、撤单线程互斥；
 *   序列化推送时在 CoinTrader.sendTradePlateMessage 里 synchronized(plate) —— 两把不同的锁！
 *   严格说有缝隙：序列化时若另一线程正持有 items 锁修改，仍可能并发（plate锁与items锁不是同一把），
 *   更严谨应统一用同一把锁。面试可主动指出。
 */
@Data
@Slf4j
public class TradePlate {
    private LinkedList<TradePlateItem> items;
    //最大深度（只展示前100档；超出深度的挂单仍参与撮合，只是不在盘口展示）
    private int maxDepth = 100;
    //方向
    private ExchangeOrderDirection direction;
    private String symbol;
    public TradePlate(){

    }

    public TradePlate(String symbol,ExchangeOrderDirection direction) {
        this.direction = direction;
        this.symbol = symbol;
        items = new LinkedList<>();
    }

    /**
     * 挂单手入口：把新订单的数量累加进盘口对应价格档位
     * 【插入逻辑】线性扫描找到第一个"不如新订单价格优"的位置插入，保持盘口有序：
     *   买盘：跳过所有比新价高的档位（continue），遇到同价则累加数量，遇到更低价则插在它前面
     *   卖盘：跳过所有比新价低的档位，同理
     * 【细节】累加的是"剩余未成交量"(amount - tradedAmount)，因为启动恢复时订单可能已部分成交
     * 【细节】市价单不进盘口（没有价格无法挂档）；超出 maxDepth 的档位直接丢弃不展示
     */
    public boolean add(ExchangeOrder exchangeOrder) {
        //log.info("add TradePlate order={}",exchangeOrder);
        synchronized (items) {
            int index = 0;
            if (exchangeOrder.getType() == ExchangeOrderType.MARKET_PRICE) {
                return false;
            }
            if (exchangeOrder.getDirection() != direction) {
                return false;
            }
            if (items.size() > 0) {
                for (index = 0; index < items.size(); index++) {
                    TradePlateItem item = items.get(index);
                    if (exchangeOrder.getDirection() == ExchangeOrderDirection.BUY && item.getPrice().compareTo(exchangeOrder.getPrice()) > 0
                            || exchangeOrder.getDirection() == ExchangeOrderDirection.SELL && item.getPrice().compareTo(exchangeOrder.getPrice()) < 0) {
                        continue;
                    } else if (item.getPrice().compareTo(exchangeOrder.getPrice()) == 0) {
                        //同价位：累加剩余数量（盘口展示的是该价位的总量，不区分哪笔订单）
                        BigDecimal deltaAmount = exchangeOrder.getAmount().subtract(exchangeOrder.getTradedAmount());
                        item.setAmount(item.getAmount().add(deltaAmount));
                        return true;
                    } else {
                        break;
                    }
                }
            }
            if(index < maxDepth) {
                TradePlateItem newItem = new TradePlateItem();
                newItem.setAmount(exchangeOrder.getAmount().subtract(exchangeOrder.getTradedAmount()));
                newItem.setPrice(exchangeOrder.getPrice());
                items.add(index, newItem);
            }
        }
        return true;
    }

    public void remove(ExchangeOrder order,BigDecimal amount) {
        synchronized (items) {
            //log.info("items>>init_size={},orderPrice={}",items.size(),order.getPrice());
            for (int index = 0; index < items.size(); index++) {
                TradePlateItem item = items.get(index);
                if (item.getPrice().compareTo(order.getPrice()) == 0) {
                    item.setAmount(item.getAmount().subtract(amount));
                    if (item.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
                        items.remove(index);
                    }
                    //log.info("items>>final_size={},itemAmount={},itemPrice={}",items.size(),item.getAmount(),item.getPrice());
                    return;
                }
            }
            log.info("items>>return_size={}",items.size());
        }
    }

    public void remove(ExchangeOrder order){
        remove(order,order.getAmount().subtract(order.getTradedAmount()));
    }

    public void setItems(LinkedList<TradePlateItem> items){
        this.items = items;
    }

    public BigDecimal getHighestPrice(){
        if(items.size() == 0) {
            return BigDecimal.ZERO;
        }
        if(direction == ExchangeOrderDirection.BUY){
            return items.getFirst().getPrice();
        }
        else{
            return items.getLast().getPrice();
        }
    }

    public int getDepth(){
        return items.size();
    }


    public BigDecimal getLowestPrice(){
        if(items.size() == 0) {
            return BigDecimal.ZERO;
        }
        if(direction == ExchangeOrderDirection.BUY){
            return items.getLast().getPrice();
        }
        else{
            return items.getFirst().getPrice();
        }
    }

    /**
     * 获取委托量最大的档位
     * @return
     */
    public BigDecimal getMaxAmount(){
        if(items.size() == 0) {
            return BigDecimal.ZERO;
        }
        BigDecimal amount = BigDecimal.ZERO;
        for(TradePlateItem item:items){
            if(item.getAmount().compareTo(amount)>0){
                amount = item.getAmount();
            }
        }
        return amount;
    }

    /**
     * 获取委托量最小的档位
     * @return
     */
    public BigDecimal getMinAmount(){
        if(items.size() == 0) {
            return BigDecimal.ZERO;
        }
        BigDecimal amount = items.getFirst().getAmount();
        for(TradePlateItem item:items){
            if(item.getAmount().compareTo(amount) < 0){
                amount = item.getAmount();
            }
        }
        return amount;
    }

    public JSONObject toJSON(){
        JSONObject json = new JSONObject();
        json.put("direction",direction);
        json.put("maxAmount",getMaxAmount());
        json.put("minAmount",getMinAmount());
        json.put("highestPrice",getHighestPrice());
        json.put("lowestPrice",getLowestPrice());
        json.put("symbol",getSymbol());
        json.put("items",items);
        return json;
    }

    public JSONObject toJSON(int limit){
        JSONObject json = new JSONObject();
        json.put("direction",direction);
        json.put("maxAmount",getMaxAmount());
        json.put("minAmount",getMinAmount());
        json.put("highestPrice",getHighestPrice());
        json.put("lowestPrice",getLowestPrice());
        json.put("symbol",getSymbol());
        json.put("items",items.size() > limit ? items.subList(0,limit) : items);
        return json;
    }
}
