package com.bizzan.bitrade.handler;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aqmd.netty.annotation.HawkBean;
import com.aqmd.netty.annotation.HawkMethod;
import com.aqmd.netty.common.NettyCacheUtils;
import com.aqmd.netty.push.HawkPushServiceApi;
import com.bizzan.bitrade.entity.ChatMessageRecord;
import com.bizzan.bitrade.entity.ConfirmResult;
import com.bizzan.bitrade.entity.MessageTypeEnum;
import com.bizzan.bitrade.entity.RealTimeChatMessage;
import com.bizzan.bitrade.netty.QuoteMessage;
import com.bizzan.bitrade.utils.DateUtils;

import com.bizzan.bitrade.constant.NettyCommand;
import com.bizzan.bitrade.entity.Order;
import com.bizzan.bitrade.service.OrderService;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

/**
 * 处理Netty订阅与取消订阅
 */
/*
 * 【面试要点】OTC 场外交易聊天服务的推送层：Netty(APP 长连接) + WebSocket
 * (SimpMessagingTemplate，H5) 双通道并存，订阅模型与 market 模块相同
 * （topic->channel 集合，按 topic 群发；这里 topic 直接用 uid，实现用户级定向推送）。
 * @HawkBean/@HawkMethod 为第三方 hawk 框架注解，按 cmd 命令字路由（Netty 版 @RequestMapping）。
 * 缺点：NettyCacheUtils 的 channel 缓存是单机内存，多实例部署时连接分散在不同节点，
 * 推送会丢 —— 集群需引入 Redis pub/sub 或 MQ 做跨节点广播；
 * 另外 APNS（苹果离线推送）相关代码全部被注释掉，说明离线推送能力未完成。
 */
@HawkBean
public class NettyHandler {
    @Autowired
    private HawkPushServiceApi hawkPushService;
    @Autowired
    private OrderService orderService ;
    @Autowired
    private MessageHandler chatMessageHandler ;
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    /*
    @Autowired
    private ApnsHandler apnsHandler;
	*/
    public void subscribeTopic(Channel channel,String topic){
        String userKey = channel.id().asLongText();
        NettyCacheUtils.keyChannelCache.put(channel,userKey);
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

    /*@HawkMethod(cmd = NettyCommand.SUBSCRIBE_CHAT,version = NettyCommand.COMMANDS_VERSION)
    public QuoteMessage.SimpleResponse subscribeChat(byte[] body, ChannelHandlerContext ctx){
        JSONObject json = JSON.parseObject(new String(body));
        System.out.println("订阅："+json.toJSONString());
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        String orderId = json.getString("orderId");
        String uid = json.getString("uid");
        if(StringUtils.isEmpty(uid) || StringUtils.isEmpty(orderId)){
            response.setCode(500).setMessage("订阅失败，参数错误");
        }
        else {
            String key = orderId + "-" + uid;
            subscribeTopic(ctx.channel(),key);
            response.setCode(0).setMessage("订阅成功");
        }
        return response.build();
    }*/

    // 订阅聊天：topic 直接用 uid（用户级频道），该用户所有订单的聊天/通知都推到这个频道
    @HawkMethod(cmd = NettyCommand.SUBSCRIBE_GROUP_CHAT)
    public QuoteMessage.SimpleResponse subscribeGroupChat(byte[] body, ChannelHandlerContext ctx){
        JSONObject json = JSON.parseObject(new String(body));
        System.out.println("订阅GroupChat："+json.toJSONString());
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        String uid = json.getString("uid");
        if(StringUtils.isEmpty(uid)){
            response.setCode(500).setMessage("订阅失败，参数错误");
        }
        else {
            String key = uid;
            subscribeTopic(ctx.channel(),key);
            response.setCode(0).setMessage("订阅成功");
        }
        return response.build();
    }

    /*@HawkMethod(cmd = NettyCommand.UNSUBSCRIBE_CHAT)
    public QuoteMessage.SimpleResponse unsubscribeChat(byte[] body, ChannelHandlerContext ctx){
        System.out.println(ctx.channel().id());
        JSONObject json = JSON.parseObject(new String(body));
        String orderId = json.getString("orderId");
        String uid = json.getString("uid");
        String key = orderId+"-"+uid;
        unsubscribeTopic(ctx.channel(),key);
        apnsHandler.removeToken(uid);
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        response.setCode(0).setMessage("取消订阅成功");
        return response.build();
    }*/

    @HawkMethod(cmd = NettyCommand.UNSUBSCRIBE_GROUP_CHAT)
    public QuoteMessage.SimpleResponse unsubscribeGroupChat(byte[] body, ChannelHandlerContext ctx){
        JSONObject json = JSON.parseObject(new String(body));
        String uid = json.getString("uid");
        String key = uid;
        unsubscribeTopic(ctx.channel(),key);
        //apnsHandler.removeToken(uid);
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        response.setCode(0).setMessage("取消订阅成功");
        return response.build();
    }

    @HawkMethod(cmd = NettyCommand.SUBSCRIBE_APNS)
    public QuoteMessage.SimpleResponse subscribeApns(byte[] body, ChannelHandlerContext ctx){
        JSONObject json = JSON.parseObject(new String(body));
        System.out.println("订阅APNS推送："+json.toJSONString());
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        String token = json.getString("token");
        String uid = json.getString("uid");
        if(StringUtils.isEmpty(uid) || StringUtils.isEmpty(token)){
            response.setCode(500).setMessage("订阅失败，参数错误");
        }
        else {
            //apnsHandler.setToken(uid,token);
            response.setCode(0).setMessage("订阅成功");
        }
        return response.build();
    }

    @HawkMethod(cmd = NettyCommand.UNSUBSCRIBE_APNS)
    public QuoteMessage.SimpleResponse unsubscribeApns(byte[] body, ChannelHandlerContext ctx){
        JSONObject json = JSON.parseObject(new String(body));
        System.out.println("取消订阅APNS推送："+json.toJSONString());
        String uid = json.getString("uid");
        //apnsHandler.removeToken(uid);
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        response.setCode(0).setMessage("取消订阅成功");
        return response.build();
    }

    @HawkMethod(cmd = NettyCommand.SEND_CHAT)
    public QuoteMessage.SimpleResponse sendMessage(byte[] body, ChannelHandlerContext ctx){
        System.out.println("发送消息："+new String(body));
        RealTimeChatMessage message = JSON.parseObject(new String(body), RealTimeChatMessage.class);
        handleMessage(message);
        QuoteMessage.SimpleResponse.Builder response = QuoteMessage.SimpleResponse.newBuilder();
        response.setCode(0).setMessage("发送成功");
        return response.build();
    }

    /**
     * 推送消息
     * @param key
     * @param result
     */
    public void push(String key, Object result,short command) {
        byte[] body = JSON.toJSONString(result).getBytes();
        Set<Channel> channels = NettyCacheUtils.getChannel(key);
        if(channels!=null && channels.size() > 0) {
            System.out.println("下发消息:key="+key+",result="+JSON.toJSONString(result)+",channel size="+channels.size());
            hawkPushService.pushMsg(channels, command, body);
        }
    }

    /*
     * 【面试要点】聊天消息统一处理入口，分两类：
     * 1) NOTICE：订单状态变更通知（如"已付款"），查出订单最新状态后推送，不落库；
     * 2) NORMAL_CHAT：普通聊天，先存 MongoDB 再推送 —— "先持久化再推送"是聊天系统的
     *    标准做法，保证历史消息可查、推送失败也不丢消息（可重拉历史）。
     * 两个通道同时推：Netty 按 uid topic 推 APP，WebSocket convertAndSendToUser 推 H5。
     */
    public void handleMessage(RealTimeChatMessage message){
        if(message.getMessageType()==MessageTypeEnum.NOTICE){
            Order order =  orderService.findOneByOrderId(message.getOrderId());
            ConfirmResult result = new ConfirmResult(message.getContent(),order.getStatus().getOrdinal());
            result.setUidFrom(message.getUidFrom());
            result.setOrderId(message.getOrderId());
            result.setNameFrom(message.getNameFrom());
            //push(message.getOrderId() + "-" + message.getUidTo(),result,NettyCommand.PUSH_CHAT);
            push(message.getUidTo(),result,NettyCommand.PUSH_GROUP_CHAT);
            // Spring WebSocket 用户定向推送：实际目的地为 /user/{uid}/order-notice/{orderId}
            messagingTemplate.convertAndSendToUser(message.getUidTo(),"/order-notice/"+message.getOrderId(),result);
        }
        else if(message.getMessageType() == MessageTypeEnum.NORMAL_CHAT) {
            ChatMessageRecord chatMessageRecord = new ChatMessageRecord();
            BeanUtils.copyProperties(message, chatMessageRecord);
            chatMessageRecord.setSendTime(DateUtils.getCurrentDate().getTime());
            chatMessageRecord.setFromAvatar(message.getAvatar());
            //聊天消息保存到mogondb
            chatMessageHandler.handleMessage(chatMessageRecord);
            chatMessageRecord.setSendTimeStr(DateUtils.getDateStr(chatMessageRecord.getSendTime()));
            //发送给指定用户（客户端订阅路径：/user/+uid+/+key）
            push(message.getUidTo(),chatMessageRecord,NettyCommand.PUSH_GROUP_CHAT);
            //push(message.getOrderId() + "-" + message.getUidTo(),chatMessageRecord,NettyCommand.PUSH_CHAT);
            //apnsHandler.handleMessage(message.getUidTo(),chatMessageRecord);
            // WebSocket 用户定向推送：/user/{uid}/{orderId}，与 Netty 推送互为冗余双通道
            messagingTemplate.convertAndSendToUser(message.getUidTo(), "/" + message.getOrderId(), chatMessageRecord);
        }
    }
}
