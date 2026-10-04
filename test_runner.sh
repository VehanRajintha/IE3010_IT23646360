#!/bin/bash
# ==============================================================================
# Module:      IE3010 - Network Programming
# Student ID:  IT23646360
# Script:      test_runner.sh
# Description: Automated test harness for verifying NetMessenger protocol
# ==============================================================================

PORT=12360
SERVER_BIN="./server_6360"
LOG_FILE="netmsg_IT23646360.log"
NID="NID:6463"

echo "=================================================="
echo " Starting NetMessenger Automated Test Suite       "
echo " Target Port: $PORT | Expected NID: $NID          "
echo "=================================================="

# Check binaries
if [ ! -f "$SERVER_BIN" ]; then
    echo "[!] Compiling server using Makefile_6360..."
    make -f Makefile_6360
fi

# Launch server in background
echo "[*] Launching server on port $PORT..."
$SERVER_BIN $PORT > /dev/null 2>&1 &
SERVER_PID=$!
sleep 1

# Check if listening
if ss -tlnp 2>/dev/null | grep -q "$PORT" || netstat -tlnp 2>/dev/null | grep -q "$PORT"; then
    echo "[PASS] Server successfully listening on port $PORT"
else
    echo "[FAIL] Server is not listening on port $PORT"
    kill $SERVER_PID 2>/dev/null
    exit 1
fi

# Test 1: Registration
echo "[*] Testing user registration..."
RESP=$(printf "REGISTER test_user\nQUIT\n" | nc -w 2 127.0.0.1 $PORT | head -n 1)
echo "Server response: $RESP"
if echo "$RESP" | grep -q "OK REGISTERED test_user $NID"; then
    echo "[PASS] Registration and NID tag verified"
else
    echo "[FAIL] Unexpected registration response"
fi

# Test 2: Unregistered command rejection
echo "[*] Testing unregistered command enforcement..."
RESP_ERR=$(printf "LIST\nQUIT\n" | nc -w 2 127.0.0.1 $PORT | head -n 1)
echo "Server response: $RESP_ERR"
if echo "$RESP_ERR" | grep -q "ERR 005 REGISTRATION_REQUIRED $NID"; then
    echo "[PASS] Unregistered command correctly rejected"
else
    echo "[FAIL] Rejection failed"
fi

# Clean up
kill $SERVER_PID 2>/dev/null
echo "=================================================="
echo " Automated Verification Complete                  "
echo "=================================================="
