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

echo `date "+%Y-%m-%d %H:%M:%S"` ======================LTE汇聚任务开始==========================
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


col="
sdate,
eci,
drop_duration,
total_duration,
cell_available_ratio,
noise,
cqi0,
cqi1,
cqi2,
cqi3,
cqi4,
cqi5,
cqi6,
cqi7,
cqi8,
cqi9,
cqi10,
cqi11,
cqi12,
cqi13,
cqi14,
cqi15,
cqi_avg,
cqi_ge7,
cqi_le7,
ul_pdcp_package_drop,
ul_pdcp_package_total,
dl_pdcp_package_drop,
dl_pdcp_package_total,
ul_pdcp_package_drop_ratio,
ul_pdcp_package_drop_ratio_qci1,
dl_pdcp_package_drop_ratio_qci1,
dl_pdcp_package_discard_ratio,
ul_speed_mbps,
dl_speed_mbps,
rrc_req,
rrc_suc,
rrc_congest,
rrc_avg,
rrc_max,
erab_req,
erab_req_qci3,
erab_req_qci4,
erab_req_qci6,
erab_req_qci7,
erab_req_qci8,
erab_req_qci9,
erab_suc,
erab_suc_qci3,
erab_suc_qci4,
erab_suc_qci6,
erab_suc_qci7,
erab_suc_qci8,
erab_suc_qci9,
erab_congest,
s1_signaling_att,
s1_signaling_suc,
s1_signaling_suc_r,
erab_suc_r,
erab_suc_r_qci1,
erab_suc_r_qci2,
erab_suc_r_qci3,
erab_suc_r_qci4,
erab_suc_r_qci5,
radio_conn_suc_r,
erab_abnormal_release,
uecontext_abnormal_release,
uecontext_release,
lte_drop_r,
lte_drop,
ho_pingpang,
ho_out_suc_r,
ul_tra_mb,
dl_tra_mb,
total_tra_mb,
rrc_fail_license,
rrc_conn_suc,
ul_prb_utilization,
dl_prb_utilization,
rrc_congest_license_r,
double_link_r,
radio_resource_utilization,
sgnb_add_req,
sgnb_add_suc,
sgnb_add_suc_r,
dl_16qam_utilization,
dl_64qam_utilization,
erab_congest_radio,
erab_congest_trans,
erab_fail_ue,
erab_fail_core,
erab_fail_trans,
erab_fail_radio,
erab_fail_resource,
rrc_release_csfb,
dl_high_msc_utilization,
ul_high_msc_utilization,
csfb_suc_r,
s1_ho_out_req,
s1_ho_out_suc,
x2_ho_out_req,
x2_ho_out_suc,
erab_req_qci1,
erab_req_qci5,
erab_suc_qci1,
erab_suc_qci5,
erab_normal_qci1,
erab_abnormal_qci1,
lte_drop_r_qci1,
lru_blind,
lru_not_blind,
succoutintraenb,
attoutintraenb,
rru_puschprbassn,
rru_puschprbtot,
rru_pdschprbassn,
rru_pdschprbtot,
effectiveconnmean,
effectiveconnmax,
pdcch_signal_occupy_ratio,
rru_pdcchcceutil,
rru_pdcchcceavail,
succexecinc,
succconnreestab_nonsrccell,
rrc_reconn_rate,
enb_handover_succ_rate,
down_pdcch_ch_cce_occ_rate,
down_pdcp_sdu_avg_delay,
mr_sinrul_gt0_ratio,
mr_sinrul_gt0_ratio_fz,
mr_sinrul_gt0_ratio_fm,
vendor,
lte_wireless_drop_ratio_cell,
PDCP_SDU_VOL_UL_plmn1,
PDCP_SDU_VOL_DL_plmn1,
effectiveconnmean_plmn1,
erab_abnormal_plmn1,
erab_normal_plmn1,
PDCP_SDU_VOL_UL_plmn2,
PDCP_SDU_VOL_DL_plmn2,
effectiveconnmean_plmn2,
erab_abnormal_plmn2,
erab_normal_plmn2,
ERAB_INI_SETUP_ATT_PLMN1_QCI1,
ERAB_INI_SETUP_ATT_PLMN1_QCI2,
ERAB_INI_SETUP_ATT_PLMN1_QCI3,
ERAB_INI_SETUP_ATT_PLMN1_QCI4,
ERAB_INI_SETUP_ATT_PLMN1_QCI5,
ERAB_INI_SETUP_ATT_PLMN1_QCI6,
ERAB_INI_SETUP_ATT_PLMN1_QCI7,
ERAB_INI_SETUP_ATT_PLMN1_QCI8,
ERAB_INI_SETUP_ATT_PLMN1_QCI9,
ERAB_INI_SETUP_SUCC_PLMN1_QCI1,
ERAB_INI_SETUP_SUCC_PLMN1_QCI2,
ERAB_INI_SETUP_SUCC_PLMN1_QCI3,
ERAB_INI_SETUP_SUCC_PLMN1_QCI4,
ERAB_INI_SETUP_SUCC_PLMN1_QCI5,
ERAB_INI_SETUP_SUCC_PLMN1_QCI6,
ERAB_INI_SETUP_SUCC_PLMN1_QCI7,
ERAB_INI_SETUP_SUCC_PLMN1_QCI8,
ERAB_INI_SETUP_SUCC_PLMN1_QCI9,
ERAB_ADD_SETUP_ATT_PLMN1_QCI1,
ERAB_ADD_SETUP_ATT_PLMN1_QCI2,
ERAB_ADD_SETUP_ATT_PLMN1_QCI3,
ERAB_ADD_SETUP_ATT_PLMN1_QCI4,
ERAB_ADD_SETUP_ATT_PLMN1_QCI5,
ERAB_ADD_SETUP_ATT_PLMN1_QCI6,
ERAB_ADD_SETUP_ATT_PLMN1_QCI7,
ERAB_ADD_SETUP_ATT_PLMN1_QCI8,
ERAB_ADD_SETUP_ATT_PLMN1_QCI9,
ERAB_ADD_SETUP_SUCC_PLMN1_QCI1,
ERAB_ADD_SETUP_SUCC_PLMN1_QCI2,
ERAB_ADD_SETUP_SUCC_PLMN1_QCI3,
ERAB_ADD_SETUP_SUCC_PLMN1_QCI4,
ERAB_ADD_SETUP_SUCC_PLMN1_QCI5,
ERAB_ADD_SETUP_SUCC_PLMN1_QCI6,
ERAB_ADD_SETUP_SUCC_PLMN1_QCI7,
ERAB_ADD_SETUP_SUCC_PLMN1_QCI8,
ERAB_ADD_SETUP_SUCC_PLMN1_QCI9,
erab_abnormal_PLMN1_QCI1,
erab_abnormal_PLMN1_QCI2,
erab_abnormal_PLMN1_QCI3,
erab_abnormal_PLMN1_QCI4,
erab_abnormal_PLMN1_QCI5,
erab_abnormal_PLMN1_QCI6,
erab_abnormal_PLMN1_QCI7,
erab_abnormal_PLMN1_QCI8,
erab_abnormal_PLMN1_QCI9,
erab_normal_PLMN1_QCI1,
erab_normal_PLMN1_QCI2,
erab_normal_PLMN1_QCI3,
erab_normal_PLMN1_QCI4,
erab_normal_PLMN1_QCI5,
erab_normal_PLMN1_QCI6,
erab_normal_PLMN1_QCI7,
erab_normal_PLMN1_QCI8,
erab_normal_PLMN1_QCI9,
ERAB_INI_SETUP_ATT_PLMN2_QCI1,
ERAB_INI_SETUP_ATT_PLMN2_QCI2,
ERAB_INI_SETUP_ATT_PLMN2_QCI3,
ERAB_INI_SETUP_ATT_PLMN2_QCI4,
ERAB_INI_SETUP_ATT_PLMN2_QCI5,
ERAB_INI_SETUP_ATT_PLMN2_QCI6,
ERAB_INI_SETUP_ATT_PLMN2_QCI7,
ERAB_INI_SETUP_ATT_PLMN2_QCI8,
ERAB_INI_SETUP_ATT_PLMN2_QCI9,
ERAB_INI_SETUP_SUCC_PLMN2_QCI1,
ERAB_INI_SETUP_SUCC_PLMN2_QCI2,
ERAB_INI_SETUP_SUCC_PLMN2_QCI3,
ERAB_INI_SETUP_SUCC_PLMN2_QCI4,
ERAB_INI_SETUP_SUCC_PLMN2_QCI5,
ERAB_INI_SETUP_SUCC_PLMN2_QCI6,
ERAB_INI_SETUP_SUCC_PLMN2_QCI7,
ERAB_INI_SETUP_SUCC_PLMN2_QCI8,
ERAB_INI_SETUP_SUCC_PLMN2_QCI9,
ERAB_ADD_SETUP_ATT_PLMN2_QCI1,
ERAB_ADD_SETUP_ATT_PLMN2_QCI2,
ERAB_ADD_SETUP_ATT_PLMN2_QCI3,
ERAB_ADD_SETUP_ATT_PLMN2_QCI4,
ERAB_ADD_SETUP_ATT_PLMN2_QCI5,
ERAB_ADD_SETUP_ATT_PLMN2_QCI6,
ERAB_ADD_SETUP_ATT_PLMN2_QCI7,
ERAB_ADD_SETUP_ATT_PLMN2_QCI8,
ERAB_ADD_SETUP_ATT_PLMN2_QCI9,
ERAB_ADD_SETUP_SUCC_PLMN2_QCI1,
ERAB_ADD_SETUP_SUCC_PLMN2_QCI2,
ERAB_ADD_SETUP_SUCC_PLMN2_QCI3,
ERAB_ADD_SETUP_SUCC_PLMN2_QCI4,
ERAB_ADD_SETUP_SUCC_PLMN2_QCI5,
ERAB_ADD_SETUP_SUCC_PLMN2_QCI6,
ERAB_ADD_SETUP_SUCC_PLMN2_QCI7,
ERAB_ADD_SETUP_SUCC_PLMN2_QCI8,
ERAB_ADD_SETUP_SUCC_PLMN2_QCI9,
erab_abnormal_PLMN2_QCI1,
erab_abnormal_PLMN2_QCI2,
erab_abnormal_PLMN2_QCI3,
erab_abnormal_PLMN2_QCI4,
erab_abnormal_PLMN2_QCI5,
erab_abnormal_PLMN2_QCI6,
erab_abnormal_PLMN2_QCI7,
erab_abnormal_PLMN2_QCI8,
erab_abnormal_PLMN2_QCI9,
erab_normal_PLMN2_QCI1,
erab_normal_PLMN2_QCI2,
erab_normal_PLMN2_QCI3,
erab_normal_PLMN2_QCI4,
erab_normal_PLMN2_QCI5,
erab_normal_PLMN2_QCI6,
erab_normal_PLMN2_QCI7,
erab_normal_PLMN2_QCI8,
erab_normal_PLMN2_QCI9,
isp,
share,
rrc_max_plmn1,
rrc_max_plmn2,
dl_prb_used,
dl_prb_total,
radio_conn_suc_r_qci689,
erab_suc_r_qci689,
ul_speed_mbps_fz,
ul_speed_mbps_fm,
dl_speed_mbps_fz,
dl_speed_mbps_fm,
ul_pdcp_package_drop_qci1,
ul_pdcp_package_qci1,
dl_pdcp_package_drop_qci1,
dl_pdcp_package_qci1,
csfb_req,
csfb_suc,
macHarqDlQpsk,
macHarqDl16Qam,
macHarqDl64Qam"

colsum="
eci,
sum(drop_duration),
sum(total_duration),
case when sum(total_duration)=0 then null else 1-trim_scale(round(sum(drop_duration)/sum(total_duration),4)) end,
case when sum(coalesce(total_duration,0)-coalesce(drop_duration,0))=0 then null else trim_scale(round(sum(noise*(coalesce(total_duration,0)-coalesce(drop_duration,0)))/sum(coalesce(total_duration,0)-coalesce(drop_duration,0)),4)) end,
sum(cqi0),
sum(cqi1),
sum(cqi2),
sum(cqi3),
sum(cqi4),
sum(cqi5),
sum(cqi6),
sum(cqi7),
sum(cqi8),
sum(cqi9),
sum(cqi10),
sum(cqi11),
sum(cqi12),
sum(cqi13),
sum(cqi14),
sum(cqi15),
case when sum(cqi0+cqi1+cqi2+cqi3+cqi4+cqi5+cqi6+cqi7+cqi8+cqi9+cqi10+cqi11+cqi12+cqi13+cqi14+cqi15)=0 then null else trim_scale(round((sum(cqi1)+2*sum(cqi2)+3*sum(cqi3)+4*sum(cqi4)+5*sum(cqi5)+6*sum(cqi6)+7*sum(cqi7)+8*sum(cqi8)+9*sum(cqi9)+10*sum(cqi10)+11*sum(cqi11)+12*sum(cqi12)+13*sum(cqi13)+14*sum(cqi14)+15*sum(cqi15))/sum(cqi0+cqi1+cqi2+cqi3+cqi4+cqi5+cqi6+cqi7+cqi8+cqi9+cqi10+cqi11+cqi12+cqi13+cqi14+cqi15),4)) end,
case when sum(cqi0+cqi1+cqi2+cqi3+cqi4+cqi5+cqi6+cqi7+cqi8+cqi9+cqi10+cqi11+cqi12+cqi13+cqi14+cqi15)=0 then null else trim_scale(round(sum(cqi7+cqi8+cqi9+cqi10+cqi11+cqi12+cqi13+cqi14+cqi15)/sum(cqi0+cqi1+cqi2+cqi3+cqi4+cqi5+cqi6+cqi7+cqi8+cqi9+cqi10+cqi11+cqi12+cqi13+cqi14+cqi15),4)) end,
case when sum(cqi0+cqi1+cqi2+cqi3+cqi4+cqi5+cqi6+cqi7+cqi8+cqi9+cqi10+cqi11+cqi12+cqi13+cqi14+cqi15)=0 then null else trim_scale(round(sum(cqi0+cqi1+cqi2+cqi3+cqi4+cqi5+cqi6)/sum(cqi0+cqi1+cqi2+cqi3+cqi4+cqi5+cqi6+cqi7+cqi8+cqi9+cqi10+cqi11+cqi12+cqi13+cqi14+cqi15),4)) end,
sum(ul_pdcp_package_drop),
sum(ul_pdcp_package_total),
sum(dl_pdcp_package_drop),
sum(dl_pdcp_package_total),
case when sum(ul_pdcp_package_total)=0 then null else trim_scale(round(sum(ul_pdcp_package_drop)/sum(ul_pdcp_package_total),4)) end,
case when sum(ul_pdcp_package_qci1)=0 or sum(ul_pdcp_package_qci1) is null then trim_scale(round(avg(ul_pdcp_package_drop_ratio_qci1),4)) else trim_scale(round(sum(ul_pdcp_package_drop_qci1)/sum(ul_pdcp_package_qci1),4)) end,
case when sum(dl_pdcp_package_qci1)=0 or sum(dl_pdcp_package_qci1) is null then trim_scale(round(avg(dl_pdcp_package_drop_ratio_qci1),4)) else trim_scale(round(sum(dl_pdcp_package_drop_qci1)/sum(dl_pdcp_package_qci1),4)) end,
case when sum(dl_pdcp_package_total)=0 or sum(dl_pdcp_package_total) is null then trim_scale(round(avg(dl_pdcp_package_discard_ratio),4)) else trim_scale(round(sum(dl_pdcp_package_total*dl_pdcp_package_discard_ratio)/sum(dl_pdcp_package_total),4)) end,
case when sum(ul_speed_mbps_fm) is null or sum(ul_speed_mbps_fm)=0 then trim_scale(round(avg(ul_speed_mbps),4)) else trim_scale(round(sum(ul_speed_mbps_fz)/sum(ul_speed_mbps_fm),4)) end,
case when sum(dl_speed_mbps_fm) is null or sum(dl_speed_mbps_fm)=0 then trim_scale(round(avg(dl_speed_mbps),4)) else trim_scale(round(sum(dl_speed_mbps_fz)/sum(dl_speed_mbps_fm),4)) end,
sum(rrc_req),
sum(rrc_suc),
sum(rrc_congest),
case when sum(coalesce(total_duration,0)-coalesce(drop_duration,0))=0 then null else trim_scale(round(sum(rrc_avg*(coalesce(total_duration,0)-coalesce(drop_duration,0)))/sum(coalesce(total_duration,0)-coalesce(drop_duration,0)),4)) end,
max(rrc_max),
sum(erab_req),
sum(erab_req_qci3),
sum(erab_req_qci4),
sum(erab_req_qci6),
sum(erab_req_qci7),
sum(erab_req_qci8),
sum(erab_req_qci9),
sum(erab_suc),
sum(erab_suc_qci3),
sum(erab_suc_qci4),
sum(erab_suc_qci6),
sum(erab_suc_qci7),
sum(erab_suc_qci8),
sum(erab_suc_qci9),
sum(erab_congest),
sum(s1_signaling_att),
sum(s1_signaling_suc),
case when sum(s1_signaling_att)=0 then null else trim_scale(round(sum(s1_signaling_suc)/sum(s1_signaling_att),4)) end,
case when sum(erab_req)=0 then null else trim_scale(round(sum(erab_suc)/sum(erab_req),4)) end,
case when sum(erab_req_qci1)=0 then trim_scale(round(avg(erab_suc_r_qci1),4)) when sum(erab_req_qci1) is null then trim_scale(round(avg(erab_suc_r_qci1),4)) else trim_scale(round(sum(erab_suc_qci1)/sum(erab_req_qci1),4)) end,
case when sum(ERAB_INI_SETUP_ATT_PLMN1_QCI2+ERAB_ADD_SETUP_ATT_PLMN1_QCI2+ERAB_INI_SETUP_ATT_PLMN2_QCI2+ERAB_ADD_SETUP_ATT_PLMN2_QCI2)=0 then trim_scale(round(avg(erab_suc_r_qci2),4)) when sum(ERAB_INI_SETUP_ATT_PLMN1_QCI2+ERAB_ADD_SETUP_ATT_PLMN1_QCI2+ERAB_INI_SETUP_ATT_PLMN2_QCI2+ERAB_ADD_SETUP_ATT_PLMN2_QCI2) is null then trim_scale(round(avg(erab_suc_r_qci2),4)) else trim_scale(round(sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI2+ERAB_ADD_SETUP_SUCC_PLMN1_QCI2+ERAB_INI_SETUP_SUCC_PLMN2_QCI2+ERAB_ADD_SETUP_SUCC_PLMN2_QCI2)/sum(ERAB_INI_SETUP_ATT_PLMN1_QCI2+ERAB_ADD_SETUP_ATT_PLMN1_QCI2+ERAB_INI_SETUP_ATT_PLMN2_QCI2+ERAB_ADD_SETUP_ATT_PLMN2_QCI2),4)) end,
case when sum(erab_req_qci3)=0 then null else trim_scale(round(sum(erab_suc_qci3)/sum(erab_req_qci3),4)) end,
case when sum(erab_req_qci4)=0 then null else trim_scale(round(sum(erab_suc_qci4)/sum(erab_req_qci4),4)) end,
case when sum(erab_req_qci5)=0 then trim_scale(round(avg(erab_suc_r_qci5),4)) when sum(erab_req_qci5) is null then trim_scale(round(avg(erab_suc_r_qci5),4)) else trim_scale(round(sum(erab_suc_qci5)/sum(erab_req_qci5),4)) end,
case when sum(rrc_req)+sum(erab_req)=0 then null else trim_scale(round((sum(rrc_suc)+sum(erab_suc))/(sum(rrc_req)+sum(erab_req)),4)) end,
sum(erab_abnormal_release),
sum(uecontext_abnormal_release),
sum(uecontext_release),
case when sum(uecontext_release)=0 then null else trim_scale(round(sum(uecontext_abnormal_release)/sum(uecontext_release),4)) end,
sum(lte_drop),
sum(ho_pingpang),
case when sum(s1_ho_out_req)+sum(x2_ho_out_req)=0 then null else trim_scale(round((sum(s1_ho_out_suc)+sum(x2_ho_out_suc))/(sum(s1_ho_out_req)+sum(x2_ho_out_req)),4)) end,
sum(ul_tra_mb),
sum(dl_tra_mb),
sum(total_tra_mb),
sum(rrc_fail_license),
sum(rrc_conn_suc),
case when sum(rru_puschprbtot)=0 then trim_scale(round(avg(ul_prb_utilization),4)) when sum(rru_puschprbtot) is null then  trim_scale(round(avg(ul_prb_utilization),4)) else trim_scale(round(sum(rru_puschprbassn)/sum(rru_puschprbtot),4)) end,
case when sum(dl_prb_total)=0    then trim_scale(round(avg(dl_prb_utilization),4)) when sum(dl_prb_total)    is null then  trim_scale(round(avg(dl_prb_utilization),4)) else trim_scale(round(sum(dl_prb_used)/sum(dl_prb_total),4)) end,
case when sum(rrc_req)=0 or sum(rrc_req) is null then trim_scale(round(avg(rrc_congest_license_r),4)) else trim_scale(round(sum(rrc_congest)/sum(rrc_req),4)) end,
case when sum(coalesce(total_duration,0)-coalesce(drop_duration,0))=0 then null else trim_scale(round(sum(double_link_r*(coalesce(total_duration,0)-coalesce(drop_duration,0)))/sum(coalesce(total_duration,0)-coalesce(drop_duration,0)),4)) end,
case when sum(coalesce(total_duration,0)-coalesce(drop_duration,0))=0 then null else trim_scale(round(sum(radio_resource_utilization*(coalesce(total_duration,0)-coalesce(drop_duration,0)))/sum(coalesce(total_duration,0)-coalesce(drop_duration,0)),4)) end,
sum(sgnb_add_req),
sum(sgnb_add_suc),
case when sum(sgnb_add_req)=0 then null else trim_scale(round(sum(sgnb_add_suc)/sum(sgnb_add_req),4)) end,
case when COALESCE(sum(macHarqDlQpsk),0)+COALESCE(sum(macHarqDl16Qam),0)+COALESCE(sum(macHarqDl64Qam),0)=0 then trim_scale(round(avg(dl_16qam_utilization),4)) else trim_scale(round(sum(macHarqDl16Qam)/(COALESCE(sum(macHarqDlQpsk),0)+COALESCE(sum(macHarqDl16Qam),0)+COALESCE(sum(macHarqDl64Qam),0)),4)) end,
case when COALESCE(sum(macHarqDlQpsk),0)+COALESCE(sum(macHarqDl16Qam),0)+COALESCE(sum(macHarqDl64Qam),0)=0 then trim_scale(round(avg(dl_64qam_utilization),4)) else trim_scale(round(sum(macHarqDl64Qam)/(COALESCE(sum(macHarqDlQpsk),0)+COALESCE(sum(macHarqDl16Qam),0)+COALESCE(sum(macHarqDl64Qam),0)),4)) end,
sum(erab_congest_radio),
sum(erab_congest_trans),
sum(erab_fail_ue),
sum(erab_fail_core),
sum(erab_fail_trans),
sum(erab_fail_radio),
sum(erab_fail_resource),
sum(rrc_release_csfb),
trim_scale(round(avg(dl_high_msc_utilization),4)),
trim_scale(round(avg(ul_high_msc_utilization),4)),
case when sum(csfb_req)=0 or sum(csfb_req) is null then trim_scale(round(avg(csfb_suc_r),4)) else trim_scale(round(sum(csfb_req)/sum(csfb_req),4)) end,
sum(s1_ho_out_req),
sum(s1_ho_out_suc),
sum(x2_ho_out_req),
sum(x2_ho_out_suc),
sum(erab_req_qci1),
sum(erab_req_qci5),
sum(erab_suc_qci1),
sum(erab_suc_qci5),
sum(erab_normal_qci1),
sum(erab_abnormal_qci1),
case when sum(erab_normal_qci1+erab_abnormal_qci1)=0 then null else trim_scale(round(sum(erab_abnormal_qci1)/sum(erab_normal_qci1+erab_abnormal_qci1),4)) end,
sum(lru_blind),
sum(lru_not_blind),
sum(succoutintraenb),
sum(attoutintraenb),
sum(rru_puschprbassn*total_duration)/sum(total_duration),
sum(rru_puschprbtot*total_duration)/sum(total_duration),
sum(rru_pdschprbassn*total_duration)/sum(total_duration),
sum(rru_pdschprbtot*total_duration)/sum(total_duration),
case when sum(coalesce(total_duration,0)-coalesce(drop_duration,0))=0 then null else trim_scale(round(sum(effectiveconnmean*(coalesce(total_duration,0)-coalesce(drop_duration,0)))/sum(coalesce(total_duration,0)-coalesce(drop_duration,0)),4)) end,
max(effectiveconnmax),
case when avg(rru_pdcchcceavail)=0 then null else trim_scale(round(avg(rru_pdcchcceutil)/avg(rru_pdcchcceavail),4))  end,
case when sum(coalesce(total_duration,0)-coalesce(drop_duration,0))=0 then null else trim_scale(round(sum(rru_pdcchcceutil*(coalesce(total_duration,0)-coalesce(drop_duration,0)))/sum(coalesce(total_duration,0)-coalesce(drop_duration,0)),4)) end,
case when sum(coalesce(total_duration,0)-coalesce(drop_duration,0))=0 then null else trim_scale(round(sum(rru_pdcchcceavail*(coalesce(total_duration,0)-coalesce(drop_duration,0)))/sum(coalesce(total_duration,0)-coalesce(drop_duration,0)),4)) end,
sum(succexecinc),
sum(succconnreestab_nonsrccell),
case when sum(rrc_req)=0 then null else trim_scale(round(sum(rrc_req*rrc_reconn_rate)/sum(rrc_req),4)) end,
case when sum(attoutintraenb)=0 then null else trim_scale(round(sum(succoutintraenb)/sum(attoutintraenb),4)) end,
case when avg(rru_pdcchcceavail)=0 then null else trim_scale(round(avg(rru_pdcchcceutil)/avg(rru_pdcchcceavail),4))  end,
case when sum(coalesce(total_duration,0)-coalesce(drop_duration,0))=0 then null else trim_scale(round(sum(down_pdcp_sdu_avg_delay*(coalesce(total_duration,0)-coalesce(drop_duration,0)))/sum(coalesce(total_duration,0)-coalesce(drop_duration,0)),4)) end,
case when sum(mr_sinrul_gt0_ratio_fm)=0 then null else trim_scale(round(sum(mr_sinrul_gt0_ratio_fz)/sum(mr_sinrul_gt0_ratio_fm),4))  end,
sum(mr_sinrul_gt0_ratio_fz),
sum(mr_sinrul_gt0_ratio_fm),
vendor,
case when sum(uecontext_release)=0 then null else trim_scale(round(sum(uecontext_abnormal_release)/sum(uecontext_release),4)) end,
sum(PDCP_SDU_VOL_UL_plmn1),
sum(PDCP_SDU_VOL_DL_plmn1),
sum(effectiveconnmean_plmn1),
sum(erab_abnormal_plmn1),
sum(erab_normal_plmn1),
sum(PDCP_SDU_VOL_UL_plmn2),
sum(PDCP_SDU_VOL_DL_plmn2),
sum(effectiveconnmean_plmn2),
sum(erab_abnormal_plmn2),
sum(erab_normal_plmn2),
sum(ERAB_INI_SETUP_ATT_PLMN1_QCI1),
sum(ERAB_INI_SETUP_ATT_PLMN1_QCI2),
sum(ERAB_INI_SETUP_ATT_PLMN1_QCI3),
sum(ERAB_INI_SETUP_ATT_PLMN1_QCI4),
sum(ERAB_INI_SETUP_ATT_PLMN1_QCI5),
sum(ERAB_INI_SETUP_ATT_PLMN1_QCI6),
sum(ERAB_INI_SETUP_ATT_PLMN1_QCI7),
sum(ERAB_INI_SETUP_ATT_PLMN1_QCI8),
sum(ERAB_INI_SETUP_ATT_PLMN1_QCI9),
sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI1),
sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI2),
sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI3),
sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI4),
sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI5),
sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI6),
sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI7),
sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI8),
sum(ERAB_INI_SETUP_SUCC_PLMN1_QCI9),
sum(ERAB_ADD_SETUP_ATT_PLMN1_QCI1),
sum(ERAB_ADD_SETUP_ATT_PLMN1_QCI2),
sum(ERAB_ADD_SETUP_ATT_PLMN1_QCI3),
sum(ERAB_ADD_SETUP_ATT_PLMN1_QCI4),
sum(ERAB_ADD_SETUP_ATT_PLMN1_QCI5),
sum(ERAB_ADD_SETUP_ATT_PLMN1_QCI6),
sum(ERAB_ADD_SETUP_ATT_PLMN1_QCI7),
sum(ERAB_ADD_SETUP_ATT_PLMN1_QCI8),
sum(ERAB_ADD_SETUP_ATT_PLMN1_QCI9),
sum(ERAB_ADD_SETUP_SUCC_PLMN1_QCI1),
sum(ERAB_ADD_SETUP_SUCC_PLMN1_QCI2),
sum(ERAB_ADD_SETUP_SUCC_PLMN1_QCI3),
sum(ERAB_ADD_SETUP_SUCC_PLMN1_QCI4),
sum(ERAB_ADD_SETUP_SUCC_PLMN1_QCI5),
sum(ERAB_ADD_SETUP_SUCC_PLMN1_QCI6),
sum(ERAB_ADD_SETUP_SUCC_PLMN1_QCI7),
sum(ERAB_ADD_SETUP_SUCC_PLMN1_QCI8),
sum(ERAB_ADD_SETUP_SUCC_PLMN1_QCI9),
sum(erab_abnormal_PLMN1_QCI1),
sum(erab_abnormal_PLMN1_QCI2),
sum(erab_abnormal_PLMN1_QCI3),
sum(erab_abnormal_PLMN1_QCI4),
sum(erab_abnormal_PLMN1_QCI5),
sum(erab_abnormal_PLMN1_QCI6),
sum(erab_abnormal_PLMN1_QCI7),
sum(erab_abnormal_PLMN1_QCI8),
sum(erab_abnormal_PLMN1_QCI9),
sum(erab_normal_PLMN1_QCI1),
sum(erab_normal_PLMN1_QCI2),
sum(erab_normal_PLMN1_QCI3),
sum(erab_normal_PLMN1_QCI4),
sum(erab_normal_PLMN1_QCI5),
sum(erab_normal_PLMN1_QCI6),
sum(erab_normal_PLMN1_QCI7),
sum(erab_normal_PLMN1_QCI8),
sum(erab_normal_PLMN1_QCI9),
sum(ERAB_INI_SETUP_ATT_PLMN2_QCI1),
sum(ERAB_INI_SETUP_ATT_PLMN2_QCI2),
sum(ERAB_INI_SETUP_ATT_PLMN2_QCI3),
sum(ERAB_INI_SETUP_ATT_PLMN2_QCI4),
sum(ERAB_INI_SETUP_ATT_PLMN2_QCI5),
sum(ERAB_INI_SETUP_ATT_PLMN2_QCI6),
sum(ERAB_INI_SETUP_ATT_PLMN2_QCI7),
sum(ERAB_INI_SETUP_ATT_PLMN2_QCI8),
sum(ERAB_INI_SETUP_ATT_PLMN2_QCI9),
sum(ERAB_INI_SETUP_SUCC_PLMN2_QCI1),
sum(ERAB_INI_SETUP_SUCC_PLMN2_QCI2),
sum(ERAB_INI_SETUP_SUCC_PLMN2_QCI3),
sum(ERAB_INI_SETUP_SUCC_PLMN2_QCI4),
sum(ERAB_INI_SETUP_SUCC_PLMN2_QCI5),
sum(ERAB_INI_SETUP_SUCC_PLMN2_QCI6),
sum(ERAB_INI_SETUP_SUCC_PLMN2_QCI7),
sum(ERAB_INI_SETUP_SUCC_PLMN2_QCI8),
sum(ERAB_INI_SETUP_SUCC_PLMN2_QCI9),
sum(ERAB_ADD_SETUP_ATT_PLMN2_QCI1),
sum(ERAB_ADD_SETUP_ATT_PLMN2_QCI2),
sum(ERAB_ADD_SETUP_ATT_PLMN2_QCI3),
sum(ERAB_ADD_SETUP_ATT_PLMN2_QCI4),
sum(ERAB_ADD_SETUP_ATT_PLMN2_QCI5),
sum(ERAB_ADD_SETUP_ATT_PLMN2_QCI6),
sum(ERAB_ADD_SETUP_ATT_PLMN2_QCI7),
sum(ERAB_ADD_SETUP_ATT_PLMN2_QCI8),
sum(ERAB_ADD_SETUP_ATT_PLMN2_QCI9),
sum(ERAB_ADD_SETUP_SUCC_PLMN2_QCI1),
sum(ERAB_ADD_SETUP_SUCC_PLMN2_QCI2),
sum(ERAB_ADD_SETUP_SUCC_PLMN2_QCI3),
sum(ERAB_ADD_SETUP_SUCC_PLMN2_QCI4),
sum(ERAB_ADD_SETUP_SUCC_PLMN2_QCI5),
sum(ERAB_ADD_SETUP_SUCC_PLMN2_QCI6),
sum(ERAB_ADD_SETUP_SUCC_PLMN2_QCI7),
sum(ERAB_ADD_SETUP_SUCC_PLMN2_QCI8),
sum(ERAB_ADD_SETUP_SUCC_PLMN2_QCI9),
sum(erab_abnormal_PLMN2_QCI1),
sum(erab_abnormal_PLMN2_QCI2),
sum(erab_abnormal_PLMN2_QCI3),
sum(erab_abnormal_PLMN2_QCI4),
sum(erab_abnormal_PLMN2_QCI5),
sum(erab_abnormal_PLMN2_QCI6),
sum(erab_abnormal_PLMN2_QCI7),
sum(erab_abnormal_PLMN2_QCI8),
sum(erab_abnormal_PLMN2_QCI9),
sum(erab_normal_PLMN2_QCI1),
sum(erab_normal_PLMN2_QCI2),
sum(erab_normal_PLMN2_QCI3),
sum(erab_normal_PLMN2_QCI4),
sum(erab_normal_PLMN2_QCI5),
sum(erab_normal_PLMN2_QCI6),
sum(erab_normal_PLMN2_QCI7),
sum(erab_normal_PLMN2_QCI8),
sum(erab_normal_PLMN2_QCI9),
isp,
max(share),
max(rrc_max_plmn1),
max(rrc_max_plmn1),
trim_scale(round(sum(dl_prb_used*total_duration)/sum(total_duration),4)),
trim_scale(round(sum(dl_prb_total*total_duration)/sum(total_duration),4)),
case when sum(coalesce(rrc_req,0))=0 or sum(coalesce(erab_req_qci6,0)+coalesce(erab_req_qci8,0)+coalesce(erab_req_qci9,0))=0 then null else trim_scale(round(sum(rrc_suc)/sum(rrc_req)*sum(coalesce(erab_suc_qci6,0)+coalesce(erab_suc_qci8,0)+coalesce(erab_suc_qci9,0))/sum(coalesce(erab_req_qci6,0)+coalesce(erab_req_qci8,0)+coalesce(erab_req_qci9,0)),4)) end,
case when sum(coalesce(erab_req_qci6,0)+coalesce(erab_req_qci8,0)+coalesce(erab_req_qci9,0))=0 then null else trim_scale(round(sum(coalesce(erab_suc_qci6,0)+coalesce(erab_suc_qci8,0)+coalesce(erab_suc_qci9,0))/sum(coalesce(erab_req_qci6,0)+coalesce(erab_req_qci8,0)+coalesce(erab_req_qci9,0)),4)) end,
sum(ul_speed_mbps_fz),
sum(ul_speed_mbps_fm),
sum(dl_speed_mbps_fz),
sum(dl_speed_mbps_fm),
sum(ul_pdcp_package_drop_qci1),
sum(ul_pdcp_package_qci1),
sum(dl_pdcp_package_drop_qci1),
sum(dl_pdcp_package_qci1),
sum(csfb_req),
sum(csfb_suc),
sum(macHarqDlQpsk),
sum(macHarqDl16Qam),
sum(macHarqDl64Qam)"

#数据库连接密码
export PGPASSWORD=abc123!




echo `date "+%Y-%m-%d %H:%M:%S"` 汇聚小时时间段：$shour------$ehour
echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_4g_hour对应时段数据
sql="delete from pm_parse.pm_4g_hour where sdate>=to_timestamp('$shour','yyyymmddhh24') and sdate<to_timestamp('$ehour','yyyymmddhh24')"
echo $sql
psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"

echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_4g_hour分区数据
sql="insert into pm_parse.pm_4g_hour select date_trunc('h',sdate),
	${colsum}
	from pm_parse.pm_4g_quater
	where sdate>=to_timestamp('$shour','yyyymmddhh24') and sdate<to_timestamp('$ehour','yyyymmddhh24')
	group by date_trunc('h',sdate),eci,vendor,isp;
	"
echo $sql
psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"

if [[ "$ehour" == *00 ]];then
	sdate=`date -d "${ehour:0:8} -1 days" "+%Y%m%d%H"`
	edate=${ehour}
	echo `date "+%Y-%m-%d %H:%M:%S"` 汇聚天时间段：$sdate------$edate
       	echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_4g_bhour分区数据
	sql="delete from pm_parse.pm_4g_bhour where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')"
	echo $sql
	psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
	echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_4g_bhour分区数据
	sql="
	insert into pm_parse.pm_4g_bhour
	select $col
	from ( select row_number() over(partition by eci order by total_tra_mb desc) rn,* from 
	pm_parse.pm_4g_hour where sdate >= to_timestamp('$sdate','yyyymmddhh24') and sdate < to_timestamp('$edate','yyyymmddhh24')) aa 
	where rn=1;
	"
	echo $sql
	psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
       	echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_4g_day分区数据
	sql="delete from pm_parse.pm_4g_day where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')"
	echo $sql
	psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
	echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_4g_day分区数据
	sql="insert into pm_parse.pm_4g_day select date_trunc('day',sdate),
	${colsum}
	from pm_parse.pm_4g_hour
	where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')
	group by date_trunc('day',sdate),eci,vendor,isp;
	"
	echo $sql
	psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"

	#备份15分钟指标（流量、prb利用率）到pm_parse.pm_4g_quater_lbl 
	sql="INSERT INTO pm_parse.pm_4g_quater_lbl 
	SELECT sdate,eci,ul_tra_mb,dl_tra_mb,total_tra_mb,ul_prb_utilization,dl_prb_utilization,isp,vendor 
		FROM pm_parse.pm_4g_quater 
		WHERE isp = '联通' and sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24');"
	echo $sql
	psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"

	#判断下个时段是不是周一
	echo `date "+%Y-%m-%d %H:%M:%S"` 下个执行时段在周：`date -d "${ehour:0:8}" "+%w"`
	if [ `date -d "${ehour:0:8}" "+%w "` -eq 1 ];then
		sdate=`date -d "${ehour:0:8} -7 days" "+%Y%m%d%H"`
		edate=${ehour}
		echo `date "+%Y-%m-%d %H:%M:%S"` 汇聚周时间段：$sdate------$edate
		echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_4g_week分区数据
		sql="delete from pm_parse.pm_4g_week where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')"
		echo $sql
		psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
		echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_4g_week分区数据
		sql="insert into pm_parse.pm_4g_week select date_trunc('week',sdate),
		${colsum}
		from pm_parse.pm_4g_day
		where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')
		group by date_trunc('week',sdate),eci,vendor,isp;
		"
		echo $sql
		psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
	fi

	#判断下个时段是不是下月1号
	if [[ "$ehour" == *0100 ]];then
		sdate=`date -d "${ehour:0:8} -1 months" "+%Y%m%d%H"`
		edate=${ehour}
		echo `date "+%Y-%m-%d %H:%M:%S"` 汇聚月时间段：$sdate------$edate
		echo `date "+%Y-%m-%d %H:%M:%S"` 清空pm_parse.pm_4g_month分区数据
		sql="delete from pm_parse.pm_4g_month where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')"
		echo $sql
		psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
		echo `date "+%Y-%m-%d %H:%M:%S"` 插入pm_parse.pm_4g_month分区数据
		sql="insert into pm_parse.pm_4g_month select date_trunc('month',sdate),
		${colsum}
		from pm_parse.pm_4g_day
		where sdate>=to_timestamp('$sdate','yyyymmddhh24') and sdate<to_timestamp('$edate','yyyymmddhh24')
		group by date_trunc('month',sdate),eci,vendor,isp;
		"
		echo $sql
		psql -U pmparse -h172.16.103.7 -p5432 -dsqmmt -c "$sql"
	fi

fi


echo `date "+%Y-%m-%d %H:%M:%S"` LTE汇聚任务完成

