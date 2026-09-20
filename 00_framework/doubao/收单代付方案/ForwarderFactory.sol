// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/proxy/Clones.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "./interfaces/IForwarder.sol";
import "./Forwarder.sol";



// @title ForwarderFactory
// @notice 用于批量部署 Forwarder 合约，支持 CREATE2 计算可预测地址
contract ForwarderFactory is Ownable {
    using Clones for address;


    // @notice 部署 clone 并初始化事件
    event CloneDeployed(address indexed implementation, bytes32 indexed salt, address clone);
    
    // @notice 部署 clone 并初始化后立刻转发 ETH
    event CloneDeployedAndFlushedETH(address indexed implementation, bytes32 indexed salt, address clone);
    
    
     // @notice 部署 clone 并初始化后立刻转发 ERC20
    event CloneDeployedAndFlushedERC20(address indexed implementation, bytes32 indexed salt, address clone, address token);
   
    // @notice 直接部署一个完整的 Forwarder (非 clone)
    event FullForwarderDeployed(bytes32 indexed salt, address forwarder);

    // @param owner_ 工厂的管理员（Ownable 模式）
    constructor() Ownable(msg.sender) {}



    // @notice 使用 clone+init 的方式部署转发器
    // @param implementation 实现合约地址
    // @param salt CREATE2 盐值
    // @return clone 部署出的 clone 合约地址
    function deployCloneAndInit(address implementation, bytes32 salt) public onlyOwner returns (address) {
        require(implementation != address(0), "impl zero");

        // 使用 cloneDeterministic (CREATE2) 部署
        address clone = implementation.cloneDeterministic(salt);

        // 从实现合约里获取 destination
        address destination = IDelegator(implementation).destination();
        require(destination != address(0), "dest zero");

        // 初始化 clone
        IForwarder(clone).init(destination);

        emit CloneDeployed(implementation, salt, clone);
        return clone;
    }

    // @notice 部署 clone 并初始化后，自动转发 ETH
    function deployCloneInitAndFlushETH(address implementation, bytes32 salt) external onlyOwner returns (address) {
        address clone = deployCloneAndInit(implementation, salt);
        IForwarder(clone).flushETH();
        emit CloneDeployedAndFlushedETH(implementation, salt, clone);
        return clone;
    }

    // @notice 部署 clone 并初始化后，自动转发 ERC20
    function deployCloneInitAndFlushERC20(address implementation, bytes32 salt, address token) external onlyOwner returns (address) {
        address clone = deployCloneAndInit(implementation, salt);
        IForwarder(clone).flushERC20(token);
        emit CloneDeployedAndFlushedERC20(implementation, salt, clone, token);
        return clone;
    }


    // @notice 使用 new Forwarder + CREATE2 部署完整 Forwarder
    function deployFullForwarder(address implementationDelegator, bytes32 salt) external onlyOwner returns (address) {
        address destination = IDelegator(implementationDelegator).destination();
        require(destination != address(0), "dest zero");

        Forwarder fwd = new Forwarder{salt: salt}(destination);
        emit FullForwarderDeployed(salt, address(fwd));
        return address(fwd);
    }


    // @notice 帮助转发器执行 ETH 转发
    function flushETHOnForwarder(address forwarder) external onlyOwner {
        IForwarder(forwarder).flushETH();
    }

    // @notice 帮助转发器执行 ERC20 转发
    function flushERC20OnForwarder(address forwarder, address token) external onlyOwner {
        IForwarder(forwarder).flushERC20(token);
    }


    // @notice 预测 clone 地址 (不需要部署)
    function predictCloneAddress(address implementation, bytes32 salt, address deployer) external pure returns (address) {
        return Clones.predictDeterministicAddress(implementation, salt, deployer);
    }


    // @notice 通用 CREATE2 地址计算
    function computeCreate2Address(bytes32 salt, bytes32 initCodeHash, address deployer) external pure returns (address) {
        bytes32 data = keccak256(abi.encodePacked(bytes1(0xff), deployer, salt, initCodeHash));
        return address(uint160(uint256(data)));
    }
}
