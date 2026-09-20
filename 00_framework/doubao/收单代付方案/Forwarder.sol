// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";


// @title Forwarder
// @notice 一个中转钱包合约，可以接收 ETH/ERC20 并一键转发到指定的目标地址
contract Forwarder {

    // @notice 目标转发地址
    address public destination;

    // @notice 标记是否已经初始化过
    bool public initialized;


    // @notice 初始化事件
    event Initialized(address indexed destination);

    // @notice ETH 转发事件
    event FlushedETH(uint256 amount);

    // @notice ERC20 转发事件
    event FlushedERC20(address indexed token, uint256 amount);



    // @notice 构造函数，可在部署时直接指定目标地址
    // @param destination_ 转发目标地址
    constructor(address destination_) payable {
        if (destination_ != address(0)) {
            destination = destination_;
            initialized = true;
            emit Initialized(destination_);
        }
    }



    // @notice 初始化函数，只能调用一次
    // @param destination_ 转发目标地址
    function init(address destination_) external {
        require(!initialized, "Forwarder: already initialized");
        require(destination_ != address(0), "Forwarder: zero destination");
        destination = destination_;
        initialized = true;
        emit Initialized(destination_);
    }


    // @notice 接收 ETH
    receive() external payable {}

    // @notice fallback 方法，同样能接收 ETH
    fallback() external payable {}



    // @notice 转发 ETH 到目标地址
    function flushETH() external {
        require(initialized, "Forwarder: not initialized");
        address payable to = payable(destination);
        uint256 balance = address(this).balance;
        if (balance == 0) {
            emit FlushedETH(0);
            return;
        }
        (bool ok, ) = to.call{value: balance}("");
        require(ok, "Forwarder: ETH transfer failed");
        emit FlushedETH(balance);
    }



    // @notice 转发 ERC20 Token 到目标地址
    // @param token ERC20 Token 地址
    function flushERC20(address token) external {
        require(initialized, "Forwarder: not initialized");
        require(token != address(0), "Forwarder: zero token");
        IERC20 erc = IERC20(token);
        uint256 bal = erc.balanceOf(address(this));
        if (bal == 0) {
            emit FlushedERC20(token, 0);
            return;
        }
        bool ok = erc.transfer(destination, bal);
        require(ok, "Forwarder: ERC20 transfer failed");
        emit FlushedERC20(token, bal);
    }
}
