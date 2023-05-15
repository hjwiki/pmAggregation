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
# 2022-8-26 修改汇聚结果为四舍五入到小数点后4位
#########################################################################
#!/usr/bin/env bash

cd `dirname $0`

find log -name aggregation.log -size +10M -exec mv {} log/aggregation_`date "+%Y-%m-%d"`.log \;

echo `date "+%Y-%m-%d %H:%M:%S"` ======================NR汇聚任务开始==========================
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

#5Gpm表头
nrcol="provice,
sdate,
eci,
cellname,
freq,
bandwidth,
vendor,
rrc_req,
rrc_suc,
rrc_congest,
rrc_suc_ratio,
ng_suc_ratio,
ng_suc,
ng_req,
radio_suc_ratio,
ng_suc_ratio_2,
ng_suc_2,
ng_req_2,
qosflow_suc_ratio,
qosflow_req,
qosflow_suc,
ue_context_rel_total,
ue_context_rel_abnormal,
ue_context_drop_ratio,
ul_tra_mb,
dl_tra_mb,
total_tra_mb,
ul_speed_mbps,
dl_speed_mbps,
drop_duration,
cell_available_ratio,
save_duration_zaipin,
save_duration_fuhao,
save_duration_tongdao,
txpower,
max_txpower,
cqi_table1_c,
cqi_table2_c,
cqi_table1_ge10,
cqi_table2_ge7,
cqi_high_ratio,
ul_noise,
ul_pdcp_package_total,
ul_pdcp_package_drop,
ul_pdcp_package_drop_ratio,
dl_pdcp_package_total,
dl_pdcp_package_discard,
dl_pdcp_package_discard_ratio,
dl_radio_resource_utilization,
ul_radio_resource_utilization,
dl_rlc_user_panle_trans_duration,
ul_prb_used,
ul_prb_available,
ul_prb_utilization,
dl_prb_used,
dl_prb_available,
dl_prb_utilization,
ul_pdcp_user_panle_tra,
dl_pdcp_user_panle_tra,
ul_rl_user_panle_tra,
dl_rl_user_panle_tra,
rrc_max,
ul_pusch_prb_utilization,
dl_pdsch_prb_utilization,
rrc_avg,
user_max,
qosflow_init_req,
qosflow_init_suc,
qosflow_init_suc_ratio,
total_duration,
NbrRbUl,
ConfigLayerUl,
NbrRbDl,
ConfigLayerDl,
pusch_prb_used,
pdsch_prb_used,
GENBID,
CI,
GNB_WIRELESS_DROP_RATIO,
GNB_SW_SUCC_RATIO,
KPI_PDCCHCCEOCCUPANCYRATE,
KPI_FLOWDROPRATE_CELLLEVEL,
KPI_RRCCONNREESTABRATE,
KPI_HOSUCCOUTINTERGNBRATE_NG,
KPI_HOSUCCOUTINTERGNBRATE_XN,
KPI_HOSUCCOUTINTERGNBRATE,
KPI_HOSUCCOUTINTRAGNBRATE,
KPI_HOSUCCOUTRATE_INTRAFREQ,
KPI_HOSUCCOUTRATE_INTERFREQ,
KPI_RLCNBRPKTLOSSRATEDL,
KPI_MACBLERUL,
KPI_MACBLERDL,
KPI_HARQRETRANSRATEUL,
KPI_HARQRETRANSRATEDL,
KPI_RANK2PERCENTDL,
KPI_RANK3PERCENTDL,
KPI_RANK4PERCENTDL,
KPI_QPSKPERCENTUL,
KPI_16QAMPERCENTUL,
KPI_64QAMPERCENTUL,
KPI_256QAMPERCENTUL,
KPI_QPSKPERCENTDL,
KPI_16QAMPERCENTDL,
KPI_64QAMPERCENTDL,
KPI_256QAMPERCENTDL,
KPI_AVGTHROUPERPRBUL,
KPI_AVGTHROUPERPRBDL,
KPI_AverageLayersUl,
KPI_AverageLayersDl,
KPI_MUPairPrbRateUl,
KPI_MUPairPrbRateDl,
CONTEXT_AttInitalSetup,
CONTEXT_SuccInitalSetup,
CONTEXT_FailInitalSetup,
CONTEXT_AttRelgNB,
CONTEXT_AttRelgNB_Normal,
CONTEXT_NbrLeft,
HO_SuccExecInc,
RRC_SuccConnReestab_NonSrccell,
HO_SuccOutInterCuNG,
HO_SuccOutInterCuXn,
HO_SuccOutIntraCUInterDU,
HO_SuccOutIntraDU,
HO_AttOutInterCuNG,
HO_AttOutInterCuXn,
HO_AttOutIntraCUInterDU,
HO_AttOutCUIntraDU,
RRU_PuschPrbAssn,
RRU_PuschPrbTot,
RRU_PdschPrbAssn,
RRU_PdcchCceUtil,
RRU_PdcchCceAvail,
Flow_NbrReqRelGnb,
Flow_NbrReqRelGnb_Normal,
Flow_HoAdmitFail,
Flow_NbrLeft,
Flow_NbrHoInc,
RRC_AttConnReestab,
HO_SuccOutIntraFreq,
HO_AttOutExecIntraFreq,
HO_SuccOutInterFreq,
HO_AttOutExecInterFreq,
RLC_NbrPktLossDl,
RLC_NbrPktDl,
MAC_NbrResErrTbUl,
MAC_NbrInitTbUl,
MAC_NbrTbUl,
MAC_NbrResErrTbDl,
MAC_NbrTbDl,
MAC_NbrInitTbDl,
MAC_NbrTbDl_Rank2,
MAC_NbrTbDl_Rank3,
MAC_NbrTbDl_Rank4,
MAC_NbrInitTbUl_Qpsk,
MAC_NbrInitTbUl_16Qam,
MAC_NbrInitTbUl_64Qam,
MAC_NbrInitTbUl_256Qam,
MAC_NbrInitTbDl_Qpsk,
MAC_NbrInitTbDl_16Qam,
MAC_NbrInitTbDl_64Qam,
MAC_NbrInitTbDl_256Qam,
RRU_DtchPrbAssnUl,
RRU_DtchPrbAssnDl,
RRU_PdschPrbTot,
MAC_CpOctUl,
MAC_CpOctDl,
PHY_ULMaxNL_PRB,
RRC_RedirectToLTE,
RRC_RedirectToLTE_EpsFB,
RRC_SAnNsaConnUserMean,
HoPrepAttOutEutran,
HoExeSuccOutEutran,
RwrEutranUeSucc,
isp,
ul_speed_fz,
ul_speed_fm,
dl_speed_fz,
dl_speed_fm,
PDCP_SDU_VOL_UL_plmn1,
PDCP_SDU_VOL_DL_plmn1,
rrc_avg_plmn1,
PDCP_SDU_VOL_UL_plmn2,
PDCP_SDU_VOL_DL_plmn2,
rrc_avg_plmn2,
kpi185,
kpi186,
kpi187,
kpi188,
kpi189,
kpi190,
kpi191,
kpi192,
kpi193,
kpi194,
kpi195,
kpi196,
kpi197,
kpi198,
kpi199,
qosflow_5qi5_req,
qosflow_5qi5_suc,
qosflow_5qi5_suc_ratio"

nrcolsum="eci,
pm_parse.last(cellname),
pm_parse.last(freq),
pm_parse.last(bandwidth),
pm_parse.last(vendor),
sum(rrc_req),
sum(rrc_suc),
sum(rrc_congest),
case when sum(rrc_req)=0 then 0 else trim_scale(round(sum(rrc_suc)/sum(rrc_req),4)) end,
case when sum(ng_req)=0 then 0 else trim_scale(round(sum(ng_suc)/sum(ng_req),4)) end,
sum(ng_suc),
sum(ng_req),
trim_scale(round((case when sum(rrc_req)=0 or sum(rrc_req) is null then avg(rrc_suc_ratio) else sum(rrc_suc)/sum(rrc_req) end)*(case when sum(ng_req)=0 or sum(ng_req) is null then avg(ng_suc_ratio) else sum(ng_suc)/sum(ng_req) end)*(case when sum(qosflow_init_req)=0 then avg(qosflow_init_suc_ratio) when sum(qosflow_init_req) is null then avg(qosflow_init_suc_ratio)  else sum(qosflow_init_suc)/sum(qosflow_init_req) end),4)),
case when sum(ng_req_2)=0 then 0 else trim_scale(round(sum(ng_suc_2)/sum(ng_req_2),4)) end,
sum(ng_suc_2),
sum(ng_req_2),
case when sum(qosflow_req)=0 then 0 else trim_scale(round(sum(qosflow_suc)/sum(qosflow_req),4)) end,
sum(qosflow_req),
sum(qosflow_suc),
sum(ue_context_rel_total),
sum(ue_context_rel_abnormal),
case when sum(ue_context_rel_total)=0 then 0 else trim_scale(round(sum(ue_context_rel_abnormal)/sum(ue_context_rel_total),4)) end,
sum(ul_tra_mb),
sum(dl_tra_mb),
sum(total_tra_mb),
case when sum(ul_speed_fm)=0 or sum(ul_speed_fm) is null then trim_scale(round(avg(ul_speed_mbps),4)) else trim_scale(round(sum(ul_speed_fz)/sum(ul_speed_fm),4)) end,
case when sum(dl_speed_fm)=0 or sum(dl_speed_fm) is null then trim_scale(round(avg(dl_speed_mbps),4)) else trim_scale(round(sum(dl_speed_fz)/sum(dl_speed_fm),4)) end,
sum(drop_duration),
trim_scale(round(avg(cell_available_ratio),4)),
sum(save_duration_zaipin),
sum(save_duration_fuhao),
sum(save_duration_tongdao),
trim_scale(round(avg(txpower),4)),
avg(max_txpower),
sum(cqi_table1_c),
sum(cqi_table2_c),
sum(cqi_table1_ge10),
sum(cqi_table2_ge7),
case when sum(cqi_table1_c)+sum(cqi_table2_c)=0 then trim_scale(round(avg(cqi_high_ratio),4)) when sum(cqi_table1_c)+sum(cqi_table2_c) is null then trim_scale(round(avg(cqi_high_ratio),4)) else trim_scale(round((sum(cqi_table1_ge10)+sum(cqi_table2_ge7))/( sum(cqi_table1_c)+sum(cqi_table2_c)),4)) end,
trim_scale(round(avg(ul_noise),4)),
sum(ul_pdcp_package_total),
sum(ul_pdcp_package_drop),
case when sum(ul_pdcp_package_total)=0 then 0 else trim_scale(round(sum(ul_pdcp_package_drop)/sum(ul_pdcp_package_total),4)) end,
sum(dl_pdcp_package_total),
sum(dl_pdcp_package_discard),
case when sum(dl_pdcp_package_total)=0 then 0 else trim_scale(round(sum(dl_pdcp_package_discard)/sum(dl_pdcp_package_total),4)) end,
trim_scale(round(avg(dl_radio_resource_utilization),4)),
trim_scale(round(avg(ul_radio_resource_utilization),4)),
sum(dl_rlc_user_panle_trans_duration),
sum(ul_prb_used),
sum(ul_prb_available),
case when sum(ul_prb_available)=0 then 0 else trim_scale(round(sum(ul_prb_used)/sum(ul_prb_available),4)) end,
sum(dl_prb_used),
sum(dl_prb_available),
case when sum(dl_prb_available)=0 then 0 else trim_scale(round(sum(dl_prb_used)/sum(dl_prb_available),4)) end,
sum(ul_pdcp_user_panle_tra),
sum(dl_pdcp_user_panle_tra),
sum(ul_rl_user_panle_tra),
sum(dl_rl_user_panle_tra),
max(rrc_max),
trim_scale(round(avg(ul_pusch_prb_utilization),4)),
trim_scale(round(avg(dl_pdsch_prb_utilization),4)),
trim_scale(round(avg(rrc_avg),4)),
max(user_max),
sum(qosflow_init_req),
sum(qosflow_init_suc),
case when sum(qosflow_init_req)=0 or sum(qosflow_init_req) is null then trim_scale(round(avg(qosflow_init_suc_ratio),4)) else trim_scale(round(sum(qosflow_init_suc)/sum(qosflow_init_req),4)) end,
sum(total_duration),
sum(NbrRbUl),
sum(ConfigLayerUl),
sum(NbrRbDl),
sum(ConfigLayerDl),
sum(pusch_prb_used),
sum(pdsch_prb_used),
GENBID,
CI,
case when sum(CONTEXT_SuccInitalSetup+CONTEXT_NbrLeft+HO_SuccExecInc+RRC_SuccConnReestab_NonSrccell)=0 then 0 else trim_scale(round(sum(CONTEXT_AttRelgNB-CONTEXT_AttRelgNB_Normal)/sum(CONTEXT_SuccInitalSetup+CONTEXT_NbrLeft+HO_SuccExecInc+RRC_SuccConnReestab_NonSrccell),4)) end,
case when sum(HO_AttOutInterCuNG+HO_AttOutInterCuXn+HO_AttOutIntraCUInterDU+HO_AttOutCUIntraDU)=0 then 0 else trim_scale(round(sum(HO_SuccOutInterCuNG+HO_SuccOutInterCuXn+HO_SuccOutIntraCUInterDU+HO_SuccOutIntraDU)/sum(HO_AttOutInterCuNG+HO_AttOutInterCuXn+HO_AttOutIntraCUInterDU+HO_AttOutCUIntraDU),4)) end,
case when sum(RRU_PdcchCceAvail)=0 then 0 else trim_scale(round(sum(RRU_PdcchCceUtil)/sum(RRU_PdcchCceAvail),4)) end,
case when sum( Flow_NbrLeft +qosflow_suc +Flow_NbrHoInc )=0 then 0 else trim_scale(round(sum(Flow_NbrReqRelGnb -Flow_NbrReqRelGnb_Normal +Flow_HoAdmitFail) / sum( Flow_NbrLeft +qosflow_suc+Flow_NbrHoInc),4)) end,
case when sum(RRC_AttConnReestab+rrc_req)=0 then 0 else trim_scale(round(sum(RRC_AttConnReestab)/sum(RRC_AttConnReestab+rrc_req),4)) end,
case when sum(HO_AttOutInterCuNG)=0 then 0 else trim_scale(round(sum(HO_SuccOutInterCuNG)/sum(HO_AttOutInterCuNG),4)) end,
case when sum(HO_AttOutInterCuXn)=0 then 0 else trim_scale(round(sum(HO_SuccOutInterCuXn)/sum(HO_AttOutInterCuXn),4)) end,
case when sum(HO_AttOutInterCuNG+HO_AttOutInterCuXn )=0 then 0 else trim_scale(round(sum(HO_SuccOutInterCuNG+HO_SuccOutInterCuXn)/sum(HO_AttOutInterCuNG+HO_AttOutInterCuXn ),4)) end,
case when sum(HO_AttOutIntraCUInterDU+HO_AttOutCUIntraDU)=0 then 0 else trim_scale(round(sum(HO_SuccOutIntraCUInterDU+HO_SuccOutIntraDU)/sum(HO_AttOutIntraCUInterDU+HO_AttOutCUIntraDU),4)) end,
case when sum(HO_AttOutExecIntraFreq)=0 then 0 else trim_scale(round(sum(HO_SuccOutIntraFreq)/sum(HO_AttOutExecIntraFreq),4)) end,
case when sum(HO_AttOutExecInterFreq)=0 then 0 else trim_scale(round(sum(HO_SuccOutInterFreq)/sum(HO_AttOutExecInterFreq),4)) end,
case when sum(RLC_NbrPktDl)=0 then 0 else trim_scale(round(sum(RLC_NbrPktLossDl)/sum(RLC_NbrPktDl),4)) end,
case when sum(MAC_NbrInitTbUl)=0 then 0 else  trim_scale(round(sum(MAC_NbrResErrTbUl)/sum(MAC_NbrInitTbUl),4)) end,
case when sum(MAC_NbrInitTbDl)=0 then 0 else trim_scale(round(sum(MAC_NbrResErrTbDl)/sum(MAC_NbrInitTbDl),4)) end,
case when sum(MAC_NbrTbUl)=0 then 0 else trim_scale(round(sum(MAC_NbrTbUl-MAC_NbrInitTbUl)/sum(MAC_NbrTbUl),4)) end,
case when sum(MAC_NbrTbDl)=0 then 0 else trim_scale(round(sum(MAC_NbrTbDl-MAC_NbrInitTbDl)/sum(MAC_NbrTbDl),4)) end,
case when sum(MAC_NbrTbDl)=0 then 0 else trim_scale(round(sum(MAC_NbrTbDl_Rank2)/sum(MAC_NbrTbDl),4)) end,
case when  sum(MAC_NbrTbDl)=0 then 0 else trim_scale(round(sum(MAC_NbrTbDl_Rank3)/sum(MAC_NbrTbDl),4)) end,
case when sum(MAC_NbrTbDl)=0 then 0 else trim_scale(round(sum(MAC_NbrTbDl_Rank4)/sum(MAC_NbrTbDl),4)) end,
case when sum(MAC_NbrInitTbUl)=0 then 0 else trim_scale(round(sum(MAC_NbrInitTbUl_Qpsk)/sum(MAC_NbrInitTbUl),4)) end,
case when sum(MAC_NbrInitTbUl)=0 then 0 else trim_scale(round(sum(MAC_NbrInitTbUl_16Qam)/sum(MAC_NbrInitTbUl),4)) end,
case when sum(MAC_NbrInitTbUl)=0 then 0 else trim_scale(round(sum(MAC_NbrInitTbUl_64Qam)/sum(MAC_NbrInitTbUl),4)) end,
case when sum(MAC_NbrInitTbUl)=0 then 0 else trim_scale(round(sum(MAC_NbrInitTbUl_256Qam)/sum(MAC_NbrInitTbUl),4)) end,
case when sum(MAC_NbrInitTbDl)=0 then 0 else trim_scale(round(sum(MAC_NbrInitTbDl_Qpsk)/sum(MAC_NbrInitTbDl),4)) end,
case when sum(MAC_NbrInitTbDl)=0 then 0 else trim_scale(round(sum(MAC_NbrInitTbDl_16Qam)/sum(MAC_NbrInitTbDl),4)) end,
case when sum(MAC_NbrInitTbDl)=0 then 0 else trim_scale(round(sum(MAC_NbrInitTbDl_64Qam)/sum(MAC_NbrInitTbDl),4)) end,
case when sum(MAC_NbrInitTbDl)=0 then 0 else trim_scale(round(sum(MAC_NbrInitTbDl_256Qam)/sum(MAC_NbrInitTbDl),4)) end,
case when sum(RRU_DtchPrbAssnUl)=0 then 0 else trim_scale(round(sum(MAC_CpOctUl*8)/sum(RRU_DtchPrbAssnUl),4)) end,
case when sum(RRU_DtchPrbAssnDl)=0 then 0 else trim_scale(round(sum(MAC_CpOctDl*8)/sum(RRU_DtchPrbAssnDl),4)) end,
trim_scale(round(avg(KPI_AverageLayersUl),4)),
trim_scale(round(avg(KPI_AverageLayersDl),4)),
trim_scale(round(avg(KPI_MUPairPrbRateUl),4)),
trim_scale(round(avg(KPI_MUPairPrbRateDl),4)),
sum(CONTEXT_AttInitalSetup),
sum(CONTEXT_SuccInitalSetup),
sum(CONTEXT_FailInitalSetup),
sum(CONTEXT_AttRelgNB),
sum(CONTEXT_AttRelgNB_Normal),
sum(CONTEXT_NbrLeft),
sum(HO_SuccExecInc),
sum(RRC_SuccConnReestab_NonSrccell),
sum(HO_SuccOutInterCuNG),
sum(HO_SuccOutInterCuXn),
sum(HO_SuccOutIntraCUInterDU),
sum(HO_SuccOutIntraDU),
sum(HO_AttOutInterCuNG),
sum(HO_AttOutInterCuXn),
sum(HO_AttOutIntraCUInterDU),
sum(HO_AttOutCUIntraDU),
sum(RRU_PuschPrbAssn),
sum(RRU_PuschPrbTot),
sum(RRU_PdschPrbAssn),
sum(RRU_PdcchCceUtil),
sum(RRU_PdcchCceAvail),
sum(Flow_NbrReqRelGnb),
sum(Flow_NbrReqRelGnb_Normal),
sum(Flow_HoAdmitFail),
sum(Flow_NbrLeft),
sum(Flow_NbrHoInc),
sum(RRC_AttConnReestab),
sum(HO_SuccOutIntraFreq),
sum(HO_AttOutExecIntraFreq),
sum(HO_SuccOutInterFreq),
sum(HO_AttOutExecInterFreq),
sum(RLC_NbrPktLossDl),
sum(RLC_NbrPktDl),
sum(MAC_NbrResErrTbUl),
sum(MAC_NbrInitTbUl),
sum(MAC_NbrTbUl),
sum(MAC_NbrResErrTbDl),
sum(MAC_NbrTbDl),
sum(MAC_NbrInitTbDl),
sum(MAC_NbrTbDl_Rank2),
sum(MAC_NbrTbDl_Rank3),
sum(MAC_NbrTbDl_Rank4),
sum(MAC_NbrInitTbUl_Qpsk),
sum(MAC_NbrInitTbUl_16Qam),
sum(MAC_NbrInitTbUl_64Qam),
sum(MAC_NbrInitTbUl_256Qam),
sum(MAC_NbrInitTbDl_Qpsk),
sum(MAC_NbrInitTbDl_16Qam),
sum(MAC_NbrInitTbDl_64Qam),
sum(MAC_NbrInitTbDl_256Qam),
sum(RRU_DtchPrbAssnUl),
sum(RRU_DtchPrbAssnDl),
sum(RRU_PdschPrbTot),
sum(MAC_CpOctUl),
sum(MAC_CpOctDl),
sum(PHY_ULMaxNL_PRB),
sum(RRC_RedirectToLTE),
sum(RRC_RedirectToLTE_EpsFB),
trim_scale(round(avg(RRC_SAnNsaConnUserMean),4)),
sum(HoPrepAttOutEutran),
sum(HoExeSuccOutEutran),
sum(RwrEutranUeSucc),
isp,
sum(ul_speed_fz),
sum(ul_speed_fm),
sum(dl_speed_fz),
sum(dl_speed_fm),
sum(PDCP_SDU_VOL_UL_plmn1),
sum(PDCP_SDU_VOL_DL_plmn1),
case when sum(total_duration)=0 then trim_scale(round(avg(rrc_avg_plmn1),4)) else trim_scale(round(sum(rrc_avg_plmn1*total_duration)/sum(total_duration),4)) end,
sum(PDCP_SDU_VOL_UL_plmn2),
sum(PDCP_SDU_VOL_DL_plmn2),
case when sum(total_duration)=0 then trim_scale(round(avg(rrc_avg_plmn2),4)) else trim_scale(round(sum(rrc_avg_plmn2*total_duration)/sum(total_duration),4)) end,
sum(kpi185),
sum(kpi186),
sum(kpi187),
sum(kpi188),
sum(kpi189),
case when sum(kpi189)=0 then 0 when sum(kpi189) is null then null else trim_scale(round(sum(kpi188)/sum(kpi189),4)) end,
sum(kpi191),
sum(kpi192),
case when sum(kpi192)=0 then 0 when sum(kpi192) is null then null else trim_scale(round(sum(kpi191)/sum(kpi192),4)) end,
sum(kpi194),
sum(kpi195),
case when sum(kpi195)=0 then 0 when sum(kpi195) is null then null else trim_scale(round(sum(kpi194)/sum(kpi195),4)) end,
sum(kpi197),
sum(kpi198),
case when sum(kpi198)=0 then 0 when sum(kpi198) is null then null else trim_scale(round(sum(kpi197)/sum(kpi198),4)) end,
sum(qosflow_5qi5_req),
sum(qosflow_5qi5_suc),
case when sum(qosflow_5qi5_req)=0 then 0 when sum(qosflow_5qi5_suc) is null then null else trim_scale(round(sum(qosflow_5qi5_suc)/sum(qosflow_5qi5_req)),4)) end"

echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_5g_hour对应时段数据
sql="delete from pm_parse.pm_5g_hour where sdate>=to_timestamp('$shour','yyyymmddhh24') and sdate<to_timestamp('$ehour','yyyymmddhh24')"
echo $sql
psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"

echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_5g_hour分区数据
sql="insert into pm_parse.pm_5g_hour select provice,date_trunc('h',sdate),
	${nrcolsum}
	from pm_parse.pm_5g_quater
	where sdate>=to_timestamp('$shour','yyyymmddhh24') and sdate<to_timestamp('$ehour','yyyymmddhh24')
	group by provice,date_trunc('h',sdate),eci,GENBID,CI,isp;
	"
echo $sql
psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"

if [[ "$ehour" == *00 ]];then
	sdate=`date -d "${ehour:0:8} -1 days" "+%Y%m%d%H"`
	edate=${ehour}
	echo `date "+%Y-%m-%d %H:%M:%S"` 汇聚天时间段：$sdate------$edate
	#=========没有pm_parse.pm_5g_bhour表，取消汇聚
       	#echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_5g_bhour分区数据
	#echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_5g_bhour分区数据
	#sql="
	#insert into pm_parse.pm_5g_bhour
	#select $nrcol
	#from ( select rank() over(partition by eci order by total_tra_mb desc) rn,* from 
	#pm_parse.pm_5g_hour where sdate >= to_timestamp('$sdate','yyyymmddhh24') and sdate < to_timestamp('$edate','yyyymmddhh24')) aa 
	#where rn=1;
	#"
	#echo $sql
	#psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
	#=================================

       	echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_5g_day分区数据
	sql="delete from pm_parse.pm_5g_day where sdate>=to_timestamp('$shour','yyyymmddhh24') and sdate<to_timestamp('$ehour','yyyymmddhh24')"
	echo $sql
	psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
	echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_5g_day分区数据
	sql="insert into pm_parse.pm_5g_day select provice,date_trunc('day',sdate),
	${nrcolsum}
	from pm_parse.pm_5g_hour
	where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')
	group by provice,date_trunc('day',sdate),eci,GENBID,CI,isp;
	"
	echo $sql
	psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"


	#判断下个时段是不是周一
	#echo `date "+%Y-%m-%d %H:%M:%S"` 下个执行时段在周：`date -d "${ehour:0:8}" "+%w"`
	#if [ `date -d "${ehour:0:8}" "+%w "` -eq 1 ];then
	#	sdate=`date -d "${ehour:0:8} -7 days" "+%Y%m%d%H"`
	#	edate=${ehour}
	#	echo `date "+%Y-%m-%d %H:%M:%S"` 汇聚周时间段：$sdate------$edate
	#	#echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_5g_week分区数据
	#	echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_5g_week分区数据
	#	sql="insert into pm_parse.pm_5g_week select provice,date_trunc('week',sdate),
	#	${nrcolsum}
	#	from pm_parse.pm_5g_day
	#	where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')
	#	group by provice,date_trunc('week',sdate),eci,cellname,freq,bandwidth,vendor;
	#	"
	#	echo $sql
	#	psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
	#fi

	#判断下个时段是不是下月1号
	if [[ "$ehour" == *0100 ]];then
		sdate=`date -d "${ehour:0:8} -1 months" "+%Y%m0100"`
		#edate=${ehour}
		edate=`date -d "${ehour:0:8}" "+%Y%m0100"`
		echo `date "+%Y-%m-%d %H:%M:%S"` 汇聚月时间段：$sdate------$edate
		#echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_5g_month分区数据
		sql="delete from pm_parse.pm_5g_month where sdate>=to_timestamp('$shour','yyyymmddhh24') and sdate<to_timestamp('$ehour','yyyymmddhh24')"
		echo $sql
		psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
		echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_5g_month分区数据
		sql="insert into pm_parse.pm_5g_month select provice,date_trunc('month',sdate),
		${nrcolsum}
		from pm_parse.pm_5g_day
		where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')
		group by provice,date_trunc('month',sdate),eci,GENBID,CI,isp;
		"
		echo $sql
		psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
	fi
fi


echo `date "+%Y-%m-%d %H:%M:%S"` 汇聚任务完成

