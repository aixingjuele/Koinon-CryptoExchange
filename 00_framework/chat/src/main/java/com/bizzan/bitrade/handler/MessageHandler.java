package com.bizzan.bitrade.handler;

import com.bizzan.bitrade.entity.ChatMessageRecord;
import com.bizzan.bitrade.entity.HistoryChatMessage;
import com.bizzan.bitrade.entity.HistoryMessagePage;

/*
 * 聊天消息处理接口：handleMessage 持久化消息，getHistoryMessage 分页拉取历史消息。
 * 面向接口编程，便于替换存储实现（当前为 MongoDB 的 ChatMessageHandler）。
 */
public interface MessageHandler {

    void handleMessage(ChatMessageRecord message);

    HistoryMessagePage getHistoryMessage(HistoryChatMessage message);
}
