#########################################################################
# File Name: aggregation.sh
# Author: haojian
# mail: hjwiki@gmail.com
# Created Time: 2022/4/15 17:26:56
# 用于进行数据汇聚
# $1 为延迟的小时数，如果$1是6则汇聚6小时前的数据
# 自动判断是否为天、周、月的最后一个小时，然后执行相关汇聚
# 2022-5-23 4G指标增加两列：isp、share
# 2022-5-30 修改prb利用率汇聚语法
# 2022-6-20 修改参数为时间格式 2022062010
#########################################################################
#!/usr/bin/env bash

cd `dirname $0`

find log -name aggregation.log -size +10M -exec mv {} log/aggregation_`date "+%Y-%m-%d"`.log \;

echo `date "+%Y-%m-%d %H:%M:%S"` ======================汇聚任务开始==========================
if [ ! -n "$1" ]; then
	hourdelta=8
	shour=`date -d "-$hourdelta hour" "+%Y%m%d%H"`
	ehour=`date -d "-$[$hourdelta-1] hour" "+%Y%m%d%H"`
else
	shour=$1
	date_d=${shour:0:8}
	date_H=${shour:8:2}
	date_M=00
	ehour=$(date -d "+1 hours ${date_d} ${date_H}:${date_M}" +%Y%m%d%H)
fi

sh aggregationLTE.sh $shour
sh aggregationNR.sh $shour

echo `date "+%Y-%m-%d %H:%M:%S"` 汇聚任务完成

