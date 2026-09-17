package com.bizzan.bitrade.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aqmd.netty.annotation.HawkBean;
import com.aqmd.netty.annotation.HawkMethod;
import com.aqmd.netty.common.NettyCacheUtils;
import com.aqmd.netty.push.HawkPushServiceApi;
import com.bizzan.bitrade.constant.NettyCommand;
import com.bizzan.bitrade.entity.*;
import com.bizzan.bitrade.netty.QuoteMessage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashSet;
import java.util.Set;

/**
 * 处理Netty订阅与取消订阅
 */
/*
 * 【面试要点】基于 Netty 的长连接行情推送（面向 APP 端），与 WebSocket
 * （SimpMessagingTemplate，面向 H5/PC 端）双通道并存 —— APP 用私有 TCP 长连接
 * 更省电、弱网表现更好，H5 只能用标准 WebSocket。
 * 订阅模型：发布订阅/群组推送。channel 按 topic 分组（NettyCacheUtils.storeChannel），
 * 推送时按 topic 找到 channel 集合群发（hawkPushService.pushMsg）。
 * topic 设计：symbol（公共行情：成交/K线/盘口）与 symbol+"-"+uid（个人订单状态，
 * 只有本人连接能收到自己的成交通知）两级。
 * @HawkBean/@HawkMethod 是第三方 hawk 框架注解，作用类似 Netty 版的
 * @Controller/@RequestMapping，按 cmd 命令字路由到对应方法。
 * 缺点：NettyCacheUtils 的 channel 缓存是单机内存的，多实例部署时用户连接分散在
 * 不同节点，按 topic 推送只能触达本机连接，其余节点上的用户会丢消息 ——
 * 集群方案需引入 Redis pub/sub 或 MQ 做跨节点广播（面试常考"WebSocket/长连接集群如何推送"）。
 */
@HawkBean
@Slf4j
public class NettyHandler implements MarketHandler {
    @Autowired
    private HawkPushServiceApi hawkPushService;
    private String topicOfSymbol = "SYMBOL_THUMB";

    // 订阅：维护三张映射 —— channel->userKey、topic->channel集合（群发用）、userKey->topic集合（退订清理用）
    public void subscribeTopic(Channel channel, String topic){
        String userKey = channel.id().asLongText();
        if(!NettyCacheUtils.keyChannelCache.containsKey(channel)) {
            NettyCacheUtils.keyChannelCache.put(channel, userKey);
        }
        NettyCacheUtils.storeChannel(topic,channel);
        if(NettyCacheUtils.userKey.containsKey(userKey)){
            NettyCacheUtils.userKey.get(userKey).add(topic);
        }
        else{
            Set<String> userkeys=new HashSet<>();
            userkeys.add(topic);
            NettyCacheUtils.userKey.put(userKey,userkeys);
        }
    }

    public void unsubscribeTopic(Channel channel,String topic){
        String userKey = channel.id().asLongText();
        if(NettyCacheUtils.userKey.containsKey(userKey)) {
            NettyCacheUtils.userKey.get(userKey).remove(topic);
        }
        NettyCacheUtils.keyChannelCache.remove(channel);
    }

    @HawkMethod(cmd = NettyCommand.SUBSCRIBE_SYMBOL_THUMB,version = NettyCommand.COMMANDS_VERSION)
    public QuoteMessage.SimpleResponse subscribeSymbolThumb(byte[] body, ChannelHandlerContext ctx){
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        subscribeTopic(ctx.channel(),topicOfSymbol);
        response.setCode(0).setMessage("订阅成功");
        return response.build();
    }

    @HawkMethod(cmd = NettyCommand.UNSUBSCRIBE_SYMBOL_THUMB)
    public QuoteMessage.SimpleResponse unsubscribeSymbolThumb(byte[] body, ChannelHandlerContext ctx){
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        unsubscribeTopic(ctx.channel(),topicOfSymbol);
        response.setCode(0).setMessage("取消成功");
        return response.build();
    }

    // 订阅交易对行情：symbol topic 收公共行情；若带 uid 再订阅 symbol+"-"+uid，
    // 用于接收个人订单状态推送（handleOrder 按此 topic 定向推送，他人收不到）
    @HawkMethod(cmd = NettyCommand.SUBSCRIBE_EXCHANGE)
    public QuoteMessage.SimpleResponse subscribeExchange(byte[] body, ChannelHandlerContext ctx){
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        JSONObject json = JSON.parseObject(new String(body));
        String symbol = json.getString("symbol");
        String uid = json.getString("uid");
        if(StringUtils.isNotEmpty(uid)){
            subscribeTopic(ctx.channel(),symbol+"-"+uid);
        }
        subscribeTopic(ctx.channel(),symbol);
        response.setCode(0).setMessage("订阅成功");
        return response.build();
    }

    @HawkMethod(cmd = NettyCommand.UNSUBSCRIBE_EXCHANGE)
    public QuoteMessage.SimpleResponse unsubscribeExchange(byte[] body, ChannelHandlerContext ctx){
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        JSONObject json = JSON.parseObject(new String(body));
        log.info("取消订阅Exchange："+json.toJSONString());
        String symbol = json.getString("symbol");
        String uid = json.getString("uid");
        if(StringUtils.isNotEmpty(uid)){
            unsubscribeTopic(ctx.channel(),symbol+"-"+uid);
        }
        unsubscribeTopic(ctx.channel(), symbol);
        response.setCode(0).setMessage("取消订阅成功");
        return response.build();
    }

    // MarketHandler 回调：成交产生时，向 SYMBOL_THUMB 组推行情摘要、向 symbol 组推成交明细
    @Override
    public void handleTrade(String symbol, ExchangeTrade exchangeTrade, CoinThumb thumb) {
        byte[] body = JSON.toJSONString(thumb).getBytes();
        hawkPushService.pushMsg(NettyCacheUtils.getChannel(topicOfSymbol),NettyCommand.PUSH_SYMBOL_THUMB, body);
        log.info("推送Trade:"+JSON.toJSONString(exchangeTrade));
        hawkPushService.pushMsg(NettyCacheUtils.getChannel(symbol),NettyCommand.PUSH_EXCHANGE_TRADE,JSONObject.toJSONString(exchangeTrade).getBytes());
    }

    // MarketHandler 回调：新K线生成时向 symbol 组推送
    @Override
    public void handleKLine(String symbol, KLine kLine) {
        hawkPushService.pushMsg(NettyCacheUtils.getChannel(symbol),NettyCommand.PUSH_EXCHANGE_KLINE, JSONObject.toJSONString(kLine).getBytes());
    }

    // 盘口推送（由 ExchangePushJob 批量调用）：24档盘口 + 50档深度各推一次
    public void handlePlate(String symbol,TradePlate plate){
        //log.info("推送盘口>>>>>:"+JSON.toJSONString(plate));
        //推送盘口
        hawkPushService.pushMsg(NettyCacheUtils.getChannel(symbol),NettyCommand.PUSH_EXCHANGE_PLATE, plate.toJSON(24).toJSONString().getBytes());
        //推送深度
        hawkPushService.pushMsg(NettyCacheUtils.getChannel(symbol),NettyCommand.PUSH_EXCHANGE_DEPTH, plate.toJSON(50).toJSONString().getBytes());
    }

    // 个人订单状态推送：topic = symbol + "-" + memberId，只有本人订阅了该 topic，实现定向推送
    public void handleOrder(short command, ExchangeOrder order){
        try {
            String topic = order.getSymbol() + "-" + order.getMemberId();
            log.info("推送订单:" + JSON.toJSONString(order));
            hawkPushService.pushMsg(NettyCacheUtils.getChannel(topic), command, JSON.toJSONString(order).getBytes());
        }
        catch (Exception e){
            e.printStackTrace();
            log.info("推送出错"+e);
        }
    }
}
