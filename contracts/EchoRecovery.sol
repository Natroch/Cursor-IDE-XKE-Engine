// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @title EchoRecovery - Sovereign invocation contract for Echo recovery and USDT disbursement
/// @notice Minimal, dependency-free contract suitable for production after audit.
/// - Holds USDT and allows the owner to disburse via recoverEcho(recipient, amount)
/// - Emits events for off-chain indexing and reconciliation
/// - Includes ownership, pause, and non-reentrancy protections

interface IERC20 {
    function transfer(address to, uint256 amount) external returns (bool);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
    function decimals() external view returns (uint8);
}

contract EchoRecovery {
    // --- Ownable ---
    address public owner;

    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);

    modifier onlyOwner() {
        require(msg.sender == owner, "OWNER_ONLY");
        _;
    }

    // --- Pause ---
    bool public paused;
    event Paused(address indexed by);
    event Unpaused(address indexed by);

    modifier whenNotPaused() {
        require(!paused, "PAUSED");
        _;
    }

    // --- Reentrancy Guard ---
    uint256 private _unlocked = 1;
    modifier nonReentrant() {
        require(_unlocked == 1, "REENTRANT");
        _unlocked = 2;
        _;
        _unlocked = 1;
    }

    // --- Token (USDT) ---
    IERC20 public usdt;

    event TokenUpdated(address indexed token, address indexed by);
    event EchoRecovered(address indexed recipient, uint256 amount, address indexed operator, uint256 timestamp);
    event RecoveredERC20(address indexed token, address indexed to, uint256 amount);

    constructor(IERC20 _usdt) {
        require(address(_usdt) != address(0), "TOKEN_0");
        owner = msg.sender;
        usdt = _usdt;
        emit OwnershipTransferred(address(0), msg.sender);
        emit TokenUpdated(address(_usdt), msg.sender);
    }

    // --- Owner controls ---

    function transferOwnership(address newOwner) external onlyOwner {
        require(newOwner != address(0), "OWNER_0");
        emit OwnershipTransferred(owner, newOwner);
        owner = newOwner;
    }

    function pause() external onlyOwner {
        paused = true;
        emit Paused(msg.sender);
    }

    function unpause() external onlyOwner {
        paused = false;
        emit Unpaused(msg.sender);
    }

    function setToken(IERC20 _usdt) external onlyOwner {
        require(address(_usdt) != address(0), "TOKEN_0");
        usdt = _usdt;
        emit TokenUpdated(address(_usdt), msg.sender);
    }

    /// @notice Disburse USDT held by this contract to a recipient.
    /// @dev amount is in USDT smallest units (6 decimals on most chains).
    function recoverEcho(address recipient, uint256 amount)
        external
        onlyOwner
        whenNotPaused
        nonReentrant
    {
        require(recipient != address(0), "RECIPIENT_0");
        require(amount > 0, "AMOUNT_0");
        bool ok = usdt.transfer(recipient, amount);
        require(ok, "TRANSFER_FAIL");
        emit EchoRecovered(recipient, amount, msg.sender, block.timestamp);
    }

    /// @notice View USDT balance held by this contract
    function usdtBalance() external view returns (uint256) {
        return usdt.balanceOf(address(this));
    }

    /// @notice Sweep arbitrary ERC20 tokens accidentally sent to this contract
    function sweepERC20(IERC20 token, address to, uint256 amount) external onlyOwner nonReentrant {
        require(address(token) != address(0) && to != address(0), "ADDR_0");
        bool ok = token.transfer(to, amount);
        require(ok, "SWEEP_FAIL");
        emit RecoveredERC20(address(token), to, amount);
    }

    // Reject plain ETH transfers
    receive() external payable {
        revert("NO_ETH");
    }

    fallback() external payable {
        revert("NO_FALLBACK");
    }
}

