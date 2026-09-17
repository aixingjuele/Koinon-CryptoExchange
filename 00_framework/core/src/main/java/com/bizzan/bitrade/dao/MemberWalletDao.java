package com.bizzan.bitrade.dao;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import com.bizzan.bitrade.dao.base.BaseDao;
import com.bizzan.bitrade.entity.Coin;
import com.bizzan.bitrade.entity.MemberWallet;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * 【面试重点】钱包 DAO —— 资金安全的最后一道防线。
 *
 * 设计核心：所有余额操作都是"单条原子 UPDATE"，利用 MySQL InnoDB 的行锁保证并发安全，
 * 而不是在 Java 层加 synchronized/Lock。原因：
 *   1. Java 锁是 JVM 进程级的，无法跨服务实例（交易所必然多实例部署）；
 *   2. Java 层加锁粒度难控制，锁多个钱包时容易产生死锁（如 A 锁钱包1等钱包2，B 锁钱包2等钱包1）；
 *   3. 数据库行锁由 InnoDB 统一管理，配合 WHERE 条件更新天然避免"先查后改"的并发漏洞。
 *
 * 这就是"无锁设计"在资金层的体现：Java 业务代码无锁，把并发控制下推到存储层（DB 行锁 + 条件更新）。
 *
 * 缺点（面试常问）：每条 SQL 都是一次数据库往返，高并发下 DB 是瓶颈。
 * 顶级交易所（如币安）会把账户余额也放进内存（内存账本），用 LMAX Disruptor 单线程记账，
 * DB 只做异步落库/快照，从而彻底摆脱 DB 行锁的吞吐限制。
 */
public interface MemberWalletDao extends BaseDao<MemberWallet> {

    /**
     * 增加钱包余额
     *
     * 原子加：balance = balance + :amount，由 InnoDB 行锁保证并发下不丢更新。
     * 无需 WHERE 条件——加钱不存在"余额不足"问题，所以不需要保护条件。
     * 典型场景：成交后给卖方加收入币、返佣入账。
     *
     * @param walletId
     * @param amount
     * @return 受影响行数，正常应为 1
     */
    @Transactional
    @Modifying
    @Query("update MemberWallet wallet set wallet.balance = wallet.balance + :amount where wallet.id = :walletId")
    int increaseBalance(@Param("walletId") long walletId, @Param("amount") BigDecimal amount);

    /**
     * 减少钱包余额
     *
     * 与 freezeBalance 同款的"条件更新"：WHERE balance >= :amount 保证不会扣成负数，
     * 余额不足时更新 0 行，调用方根据返回值判断失败。
     *
     * @param walletId
     * @param amount
     * @return 受影响行数，0 表示余额不足扣减失败
     */
    @Transactional
    @Modifying
    @Query("update MemberWallet wallet set wallet.balance = wallet.balance - :amount where wallet.id = :walletId and wallet.balance >= :amount")
    int decreaseBalance(@Param("walletId") long walletId, @Param("amount") BigDecimal amount);

    /**
     * 冻结钱包余额（下单时调用）
     *
     * 【核心】冻结 = 可用余额减 + 冻结余额加，一条 SQL 同时完成两个字段的修改。
     * 关键在 WHERE 里的 balance >= :amount 条件：
     *   - 余额充足：更新 1 行，冻结成功；
     *   - 余额不足：更新 0 行，Service 层根据返回值判断失败并提示用户。
     *
     * 这是典型的"数据库乐观锁 / CAS 思想"：把"检查余额 + 扣减余额"合并成一条原子 SQL，
     * 杜绝了"先 SELECT 查余额、再 UPDATE 扣减"两步操作之间的并发超卖漏洞（TOCTOU，
     * Time-Of-Check-Time-Of-Use：检查时刻和使用时刻之间余额可能已被别的线程扣掉）。
     *
     * 面试必考：如何防止并发下单超扣余额？
     * 答案就是这种条件更新（update ... where balance >= x），
     * 或者用 Redis Lua 脚本做原子扣减（EVAL 保证 check+decrby 原子执行）。
     *
     * @param walletId
     * @param amount
     * @return 受影响行数，0 表示余额不足冻结失败
     */
    @Modifying
    @Query("update MemberWallet wallet set wallet.balance = wallet.balance - :amount,wallet.frozenBalance=wallet.frozenBalance + :amount where wallet.id = :walletId and wallet.balance >= :amount")
    int freezeBalance(@Param("walletId") long walletId, @Param("amount") BigDecimal amount);

    /**
     * 解冻钱包余额（撤单/订单完成时退回未成交部分）
     *
     * 冻结余额减 + 可用余额加，WHERE frozenBalance >= :amount 防止冻结余额被扣成负数
     * （例如重复撤单、消息重复消费导致的重复解冻）。
     * 返回 0 行说明解冻金额超过了冻结金额，属于异常状态，需要人工对账介入。
     *
     * @param walletId
     * @param amount
     * @return 受影响行数，0 表示冻结余额不足，解冻失败
     */
    @Transactional
    @Modifying
    @Query("update MemberWallet wallet set wallet.balance = wallet.balance + :amount,wallet.frozenBalance=wallet.frozenBalance - :amount where wallet.id = :walletId and wallet.frozenBalance >= :amount")
    int thawBalance(@Param("walletId") long walletId, @Param("amount") BigDecimal amount);

    /**
     * 减少冻结余额（成交后真正扣款：冻结资金被划走）
     *
     * 与 thawBalance 的区别：thaw 是"退回可用"（冻结→可用），本方法是"实际扣除"（冻结直接减少，
     * 对应买方付出的币被划给卖方）。同样有 frozenBalance >= :amount 保护，防止扣成负数。
     *
     * @param walletId
     * @param amount
     * @return 受影响行数，0 表示冻结余额不足，扣减失败
     */
    @Transactional
    @Modifying
    @Query("update MemberWallet wallet set wallet.frozenBalance=wallet.frozenBalance - :amount where wallet.id = :walletId and wallet.frozenBalance >= :amount")
    int decreaseFrozen(@Param("walletId") long walletId, @Param("amount") BigDecimal amount);


    MemberWallet findByCoinAndAddress(Coin coin, String address);

    MemberWallet findByCoinAndMemberId(Coin coin, Long memberId);

    List<MemberWallet> findAllByMemberId(Long memberId);

    List<MemberWallet> findAllByCoin(Coin coin);

    @Query(value="select sum(a.balance)+sum(a.frozen_balance) as allBalance from member_wallet a where a.coin_id = :coinName",nativeQuery = true)
    BigDecimal getWalletAllBalance(@Param("coinName")String coinName);

    
    //查询快照表BHB总数
    @Query(value="select sum(a.balance) as allBalance from member_wallet_:weekDay a where a.coin_id = :coinName AND balance >=10000 AND member_id NOT IN (66946,65859,13029,55)",nativeQuery = true)
    BigDecimal getWalletBalanceAmount(@Param("coinName")String coinName,@Param("weekDay")int weekDay);
    
    
    //定时任务，筛选小于500的直接累加
    @Transactional
    @Modifying
    @Query(value = "UPDATE member_wallet SET balance=balance+to_released,to_released=0 WHERE to_released<=500 AND to_released<>0",nativeQuery = true)
    int unfreezeLess();

    /**
     * 查询待释放BHB小于500的
     */
    @Query(value = "select * from member_wallet WHERE to_released<=500 AND to_released>0",nativeQuery = true)
    List<MemberWallet> findUnfreezeLTE();

    /**
     * 查询待释放BHB大于500的
     */
    @Query(value = "select * from member_wallet WHERE to_released>500",nativeQuery = true)
    List<MemberWallet> findUnfreezeGTE();
    
    //定时任务，筛选大于500的逐步释放
    @Transactional
    @Modifying
    @Query(value = "UPDATE member_wallet SET balance=balance+500,to_released=to_released-500 WHERE to_released>500",nativeQuery = true)
    int unfreezeMore();


    //删除快照表
    @Transactional
    @Modifying
    @Query(value = "TRUNCATE TABLE member_wallet_:weekDay",nativeQuery = true)
    int dropWeekTable(@Param("weekDay")int weekDay);


    //新增快照表
    @Transactional
    @Modifying
    @Query(value = "insert INTO member_wallet_:weekDay SELECT * FROM member_wallet",nativeQuery = true)
    int createWeekTable(@Param("weekDay")int weekDay);


    //根据快照表查询每个人拥有的BHB
    @Query(value="select * from member_wallet_:weekDay a where a.coin_id = :coinName  AND balance>=10000 AND member_id NOT IN (66946,65859,13029,55)",nativeQuery = true)
    List<MemberWallet> geteveryBHB(@Param("coinName")String coinName,@Param("weekDay")int weekDay);

    @Query(value = "select * from member_wallet where  coin_id=:coinId and member_id=:memberId ",nativeQuery =true)
    MemberWallet getMemberWalletByCoinAndMemberId(@Param("coinId") String coinId, @Param("memberId") long memberId);


    @Transactional
    @Modifying
    @Query(value="UPDATE member_wallet SET balance=balance+:teamBalance where coin_id = 'BHB' AND member_id=:teamId",nativeQuery = true)
    int updateTeamWallet(@Param("teamBalance")BigDecimal teamBalance,@Param("teamId")long teamId);

    @Transactional
    @Modifying
    @Query(value="UPDATE member_wallet SET balance=balance-:normalBalance,frozen_balance=frozen_balance+:normalBalance where coin_id=:coinId AND member_id=:memberId",nativeQuery = true)
    int updateMemberWalletByMemberIdAndCoinId(@Param("normalBalance")BigDecimal normalBalance,@Param("coinId")String coinId,@Param("memberId")long memberId);

    @Transactional
    @Modifying
    @Query(value="UPDATE member_wallet SET balance=balance+:allBalance,frozen_balance=frozen_balance-:forzenBalance where coin_id=:coinId AND member_id=:memberId",nativeQuery = true)
    int updateMemberWalletByMemberIdAndCoinId(@Param("allBalance")BigDecimal allBalance,@Param("forzenBalance")BigDecimal forzenBalance,@Param("coinId")String coinId,@Param("memberId")long memberId);

    
    /**
     * 根据用户Id和币种ID更新用户钱包
     * @param memberId
     * @param coinId
     * @param balance
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    @Modifying
    @Query(value = "UPDATE member_wallet SET balance=balance+:balance WHERE coin_id=:coinId AND member_id=:memberId",nativeQuery = true)
    int updateByMemberIdAndCoinId(@Param("memberId")long memberId,@Param("coinId")String coinId,@Param("balance")BigDecimal balance);

    /**
     * 增加用户BHB余额
     * @param memberId
     * @return
     */

    @Transactional(rollbackFor = Exception.class)
    @Modifying
    @Query(value = "UPDATE member_wallet SET balance=balance+:balance WHERE coin_id='BHB' AND member_id=:memberId",nativeQuery = true)
    int increaseBalanceForBHB(@Param("balance")BigDecimal mineAmount,@Param("memberId") Long memberId);

//    //初始化超级合伙人BHB的数量
//    @Modifying
//    @Transactional
//    @Query(value="update member_wallet  set balance = '20' where member_id = :memberId and coin_id = 'BHB' ",nativeQuery = true)
//    int initSuperPaterner(@Param("memberId") long memberId);

    /**
     * 币竞猜扣件用户余额
     * @param id
     * @param amount
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    @Modifying
    @Query(value = "UPDATE member_wallet SET balance=balance-:amount WHERE id=:id and balance>=:amount",nativeQuery = true)
    int updateBalanceByIdAndAmount (@Param("id") long id,@Param("amount") double amount);

    /**
     * 增加冻结资产
     * @param id
     * @param amount
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    @Modifying
    @Query("update MemberWallet wallet set wallet.frozenBalance=wallet.frozenBalance + :amount where wallet.id = :walletId")
	int increaseFrozen(@Param("walletId") Long walletId, @Param("amount") BigDecimal amount);

    /**
     * 增加待释放资产
     * @param id
     * @param amount
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    @Modifying
    @Query("update MemberWallet wallet set wallet.toReleased=wallet.toReleased + :amount where wallet.id = :walletId")
    int increaseToRelease(@Param("walletId") Long walletId, @Param("amount") BigDecimal amount);

    /**
     * 减少待释放资产
     * @param id
     * @param amount
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    @Modifying
    @Query("update MemberWallet wallet set wallet.toReleased=wallet.toReleased - :amount where wallet.id = :walletId")
    int decreaseToRelease(@Param("walletId") Long walletId, @Param("amount") BigDecimal amount);
}
