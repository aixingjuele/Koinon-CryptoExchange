// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;


contract NativeUSDT {
    // ERC20 state
    string public name = "Native Test USDT";
    string public symbol = "nUSDT";
    uint8 public decimals = 6; // USDT uses 6 decimals
    uint256 public totalSupply;

    uint256 public constant MAX_CLAIM = 10000 * 10**6; // 每次最多领取 10000 个

    mapping(address => uint256) private _balances;
    mapping(address => mapping(address => uint256)) private _allowances;

    // Owner (deployer)
    address public owner;

    // Events
    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);

    // Modifiers
    modifier onlyOwner() {
        require(msg.sender == owner, "Only owner");
        _;
    }

    constructor() {
        owner = msg.sender;
        // optional: mint some initial tokens to owner for testing
        // _mint(msg.sender, 1000 * 10**decimals);
    }

    // ---------- ERC20 basics ----------
    function balanceOf(address account) external view returns (uint256) {
        return _balances[account];
    }

    function allowance(address _owner, address spender) external view returns (uint256) {
        return _allowances[_owner][spender];
    }

    function approve(address spender, uint256 amount) external returns (bool) {
        _allowances[msg.sender][spender] = amount;
        emit Approval(msg.sender, spender, amount);
        return true;
    }

    function transfer(address to, uint256 amount) external returns (bool) {
        _transfer(msg.sender, to, amount);
        return true;
    }

    function transferFrom(address from, address to, uint256 amount) external returns (bool) {
        uint256 allowed = _allowances[from][msg.sender];
        require(allowed >= amount, "Allowance exceeded");
        _allowances[from][msg.sender] = allowed - amount;
        _transfer(from, to, amount);
        return true;
    }

    // ---------- Internal transfer/mint/burn ----------
    function _transfer(address from, address to, uint256 amount) internal {
        require(to != address(0), "Transfer to zero");
        uint256 fromBal = _balances[from];
        require(fromBal >= amount, "Balance too low");
        _balances[from] = fromBal - amount;
        _balances[to] += amount;
        emit Transfer(from, to, amount);
    }

    function _mint(address to, uint256 amount) internal {
        require(to != address(0), "Mint to zero");
        totalSupply += amount;
        _balances[to] += amount;
        emit Transfer(address(0), to, amount);
    }

    function _burn(address from, uint256 amount) internal {
        uint256 bal = _balances[from];
        require(bal >= amount, "Burn exceeds balance");
        _balances[from] = bal - amount;
        totalSupply -= amount;
        emit Transfer(from, address(0), amount);
    }

    // ---------- Owner utilities ----------
    /// @notice Owner can mint arbitrarily (for tests/admin).
    function ownerMint(address to, uint256 amount) external onlyOwner {
        _mint(to, amount);
    }

    /// @notice Owner can change owner
    function changeOwner(address newOwner) external onlyOwner {
        require(newOwner != address(0), "zero address");
        owner = newOwner;
    }

    // ---------- Public testing faucet / claim ----------
    /// @notice Claim tokens to a target address. If `to` is address(0), tokens are minted to msg.sender.
    /// @param amount amount in token base units (i.e., include decimals). For 1.0 token with decimals=6, amount = 1 * 10**6.
    /// @param to recipient address; pass address(0) to default to msg.sender.
    function claim(uint256 amount, address to) external {
            require(amount <= MAX_CLAIM, "Claim exceeds maximum per tx");
        address recipient = to;
        if (recipient == address(0)) {
            recipient = msg.sender;
        }
        // no access control: anyone can claim (useful for local tests). If you want limits, add checks here.
        _mint(recipient, amount);
    }

    // Convenience overload: claim to self (so frontend can call claim(amount) without passing address)
    function claim(uint256 amount) external {
         require(amount <= MAX_CLAIM, "Claim exceeds maximum per tx");
        _mint(msg.sender, amount);
    }

    // ---------- Burn ----------
    function burn(uint256 amount) external {
        _burn(msg.sender, amount);
    }
}
