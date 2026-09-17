package com.bizzan.bitrade.handler;

import com.alibaba.fastjson.JSON;
import com.bizzan.bitrade.entity.ChatMessageRecord;
import com.bizzan.bitrade.entity.HistoryChatMessage;
import com.bizzan.bitrade.entity.HistoryMessagePage;
import com.bizzan.bitrade.utils.DateUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;

/*
 * 【面试要点】MessageHandler 实现：聊天消息持久化到 MongoDB（chat_message 集合），
 * 并提供按订单分页查询历史消息的能力（按时间倒序 + skip/limit 分页）。
 * 聊天系统选 MongoDB：写入量大、结构灵活、按 orderId 查询简单。
 * 注意：skip 分页在数据量大时是深分页，性能差，生产可改为"按时间戳游标"翻页。
 */
@Component
public class ChatMessageHandler implements MessageHandler {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Override
    public void handleMessage(ChatMessageRecord message) {
        mongoTemplate.insert(message, "chat_message"/*+message.getOrderId()*/);

    }

    /**
     * 获取历史聊天消息
     *
     * @param message
     * @return
     */
    @Override
    public HistoryMessagePage getHistoryMessage(HistoryChatMessage message) {
        Criteria criteria = new Criteria();
        if(!StringUtils.isEmpty(message.getOrderId())) {
            criteria = Criteria.where("orderId").is(message.getOrderId());
        }
        Sort sort = new Sort(new Sort.Order(Sort.Direction.DESC, message.getSortFiled()));
        Query query = new Query(criteria).with(sort);
        long total = mongoTemplate.count(query, ChatMessageRecord.class, "chat_message");
        query.limit(message.getLimit()).skip((message.getPage() - 1) * message.getLimit());
        List<ChatMessageRecord> list = mongoTemplate.find(query, ChatMessageRecord.class, "chat_message");
        for (ChatMessageRecord record : list) {
            record.setSendTimeStr(DateUtils.getDateStr(record.getSendTime()));
        }
        long consult = total / message.getLimit();
        long residue = total % message.getLimit();
        long totalPage = residue == 0 ? consult : (consult + 1);
        return HistoryMessagePage.getInstance(message.getPage(), totalPage, list.size(), total, list);

    }

}