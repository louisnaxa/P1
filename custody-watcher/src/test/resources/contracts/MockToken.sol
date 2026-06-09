// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * Minimal token for chaos tests.
 * Emits the standard ERC-20 Transfer event so the custody-watcher
 * can detect it via eth_getLogs — no balance tracking needed.
 */
contract MockToken {
    event Transfer(address indexed from, address indexed to, uint256 value);

    function transfer(address to, uint256 value) external returns (bool) {
        emit Transfer(msg.sender, to, value);
        return true;
    }
}
