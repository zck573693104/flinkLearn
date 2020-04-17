#!/bin/bash
echo "order_prod_process"
nohup ./prod/order_prod_process.sh >> /usr/logs/order_prod_process.log 2>&1 &
echo "order_detail_prod_process"
nohup ./prod/order_detail_prod_process.sh >> /usr/logs/order_detail_prod_process.log 2>&1 &
echo "hotItems_prod_pv_process"
nohup ./prod/hotItems_prod_pv_process.sh >> /usr/logs/hotItems_prod_pv_process.log 2>&1 &
echo "hotItems_prod_cart_process"
nohup ./prod/hotItems_prod_cart_process.sh >> /usr/logs/hotItems_prod_cart_process.log 2>&1 &
echo "hotItems_prod_searchRecord_process"
nohup ./prod/hotItems_prod_searchRecord_process.sh >> /usr/logs/hotItems_prod_searchRecord_process.log 2>&1 &