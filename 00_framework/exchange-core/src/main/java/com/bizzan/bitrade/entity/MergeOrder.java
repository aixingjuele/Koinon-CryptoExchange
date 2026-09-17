package com.bizzan.bitrade.entity;

import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 同价位订单合并列表：订单簿(TreeMap)中一个价格档位对应一个 MergeOrder
 * 【时间优先(FIFO)的载体】orders 是 ArrayList，新订单 add 到尾部，
 *   撮合时 iterator() 从头遍历 → 同价格下先挂的单先成交
 * 【为什么订单簿是 TreeMap<价格, MergeOrder> 两层结构，而不是直接 TreeMap<价格, 订单>？】
 *   一个价格可以挂多笔订单，TreeMap 的 key 唯一，所以 value 必须是列表；
 *   这也是主流订单簿的标准结构：价格档位(Level) → 档内订单队列(Queue)
 */
public class MergeOrder {
    private List<ExchangeOrder> orders = new ArrayList<>();

    //最后位置添加一个
    public void add(ExchangeOrder order){
        orders.add(order);
    }


    public ExchangeOrder get(){
        return orders.get(0);
    }

    public int size(){
        return orders.size();
    }

    public BigDecimal getPrice(){
        return orders.get(0).getPrice();
    }

    public Iterator<ExchangeOrder> iterator(){
        return orders.iterator();
    }
    
    public BigDecimal getTotalAmount() {
    	BigDecimal total = new BigDecimal(0);
    	for(ExchangeOrder item : orders) {
    		total = total.add(item.getAmount());
    	}
    	return total;
    }
}
