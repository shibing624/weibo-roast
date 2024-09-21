# -*- coding: utf-8 -*-
"""
@author:XuMing(xuming624@qq.com)
@description: 
"""

import os
import sys

sys.path.append("..")
from weibo_crawler import crawl_weibo_content_by_userids, DATA_DIR
from loguru import logger

pwd_path = os.path.abspath(os.path.dirname(__file__))

if __name__ == "__main__":
    user_ids = []
    file_path = os.path.join(pwd_path, "famous_uid.txt")
    with open(file_path, 'r') as f:
        for line in f:
            user_ids.append(line.strip())
    user_ids = list(set(user_ids))

    crawled_user_ids = []
    crawled_user_file = os.path.join(DATA_DIR, "users.csv")
    if os.path.exists(crawled_user_file):
        with open(crawled_user_file, 'r') as f:
            for line in f:
                crawled_user_ids.append(line.strip().split(",")[0])
    need_crawl_user_ids = list(set(user_ids) - set(crawled_user_ids))
    logger.info(f"input user_ids size:{len(user_ids)}, \n"
                f"crawled_user_ids size:{len(crawled_user_ids)}, \n"
                f"need_crawl_user_ids size:{len(need_crawl_user_ids)}")
    batch_size = 10
    batch_uids = [need_crawl_user_ids[i:i + batch_size] for i in range(0, len(need_crawl_user_ids), batch_size)]
    for user_ids in batch_uids:
        try:
            logger.info(f"抓取博主：{user_ids}")
            screen_names = crawl_weibo_content_by_userids(user_ids, max_blogs=15)
            logger.info(f"done, user_ids：{user_ids}，博主昵称：{screen_names}")
        except Exception as e:
            logger.error(f"抓取博主：{user_ids}，失败, {e}")
