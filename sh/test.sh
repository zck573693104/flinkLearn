#!/bin/bash
echo "order_test_process"
nohup ./test/order_test_process.sh >> /usr/logs/order_test_process.log 2>&1 &
echo "order_detail_test_process"
nohup ./test/order_detail_test_process.sh >> /usr/logs/order_detail_test_process.log 2>&1 &
echo "hotItems_test_pv_process"
nohup ./test/hotItems_test_pv_process.sh >> /usr/logs/hotItems_test_pv_process.log 2>&1 &
echo "hotItems_test_cart_process"
nohup ./test/hotItems_test_cart_process.sh >> /usr/logs/hotItems_test_cart_process.log 2>&1 &
echo "hotItems_test_searchRecord_process"
nohup ./test/hotItems_test_searchRecord_process.sh >> /usr/logs/hotItems_test_searchRecord_process.log 2>&1 &