-- db_fund.db_ttjj_basic definition

CREATE TABLE `db_ttjj_basic` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'id',
  `built_date` date DEFAULT NULL COMMENT '成立日期',
  `scale` varchar(100) DEFAULT NULL COMMENT '规模',
  `fund_code` varchar(100) DEFAULT NULL COMMENT '基金代码',
  `fund_type` varchar(100) DEFAULT NULL COMMENT '类型',
  `scale_num` double DEFAULT NULL COMMENT '规模（亿元）',
  `scale_stat_date` date DEFAULT NULL COMMENT '统计截止日期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=12993 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



-- db_fund.db_ttjj_fund_ranking definition

CREATE TABLE `db_ttjj_fund_ranking` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `fund_type` varchar(100) DEFAULT NULL COMMENT '类型',
  `update_date` date DEFAULT NULL COMMENT '获取日期',
  `fund_code` varchar(100) DEFAULT NULL,
  `fund_name` varchar(100) DEFAULT NULL COMMENT '名称',
  `fund_name_sx` varchar(100) DEFAULT NULL COMMENT '缩写',
  `fund_stat_date` date DEFAULT NULL,
  `ass_net` double DEFAULT NULL COMMENT '单位净值',
  `acc_net` double DEFAULT NULL COMMENT '累计净值',
  `d_rate` double DEFAULT NULL COMMENT '日增长率(%)',
  `w` double DEFAULT NULL COMMENT '近1周增幅',
  `m` double DEFAULT NULL COMMENT '近1月增幅',
  `m3` double DEFAULT NULL COMMENT '近3月增幅',
  `m6` double DEFAULT NULL COMMENT '近6月增幅',
  `y` double DEFAULT NULL COMMENT '近1年增幅',
  `y2` double DEFAULT NULL COMMENT '近2年增幅',
  `y3` double DEFAULT NULL COMMENT '近3年增幅',
  `in_y` double DEFAULT NULL COMMENT '今年来',
  `all_y` double DEFAULT NULL COMMENT '成立来',
  `fund_create_date` date DEFAULT NULL COMMENT '成立日期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=31876 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='天天 - 排名';


