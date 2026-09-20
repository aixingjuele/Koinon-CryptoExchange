// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

interface IForwarder {

    // @notice 初始化转发器，指定目标地址
    // @param destination_ 资金最终要转发到的目标地址
    function init(address destination_) external;

    // @notice 把合约中收到的 ETH 转发给目标地址
    function flushETH() external;

    // @notice 把合约中收到的 ERC20 Token 转发给目标地址
    // @param token ERC20 Token 合约地址
    function flushERC20(address token) external;
}


// @title IDelegator 接口
// @notice 工厂合约需要从实现合约里获取目标地址
interface IDelegator {
    // @notice 获取目标转发地址
    function destination() external view returns (address);
}
