# -*- coding: utf-8 -*-
"""
@author:XuMing(xuming624@qq.com)
@description: 
"""

import re
import requests
from loguru import logger
import json
import time


def crawl_more_famous_uid(user_ids, save_file):
    try:
        for uid in user_ids:
            for page in range(1, 12):
                try:
                    follow_url = f'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_{uid}&page={page}'
                    r = requests.get(follow_url, headers={'User-Agent': 'Mozialla/5.0'})
                    logger.debug(f'follow_url:{follow_url}, r:{r}')
                    res = r.json()
                    card_t = res.get('data').get('cards')
                except Exception as e1:
                    logger.error(f'error:{e1}')
                    continue
                if card_t and len(card_t) > 0:
                    card_group = card_t[0].get('card_group')
                    logger.debug(f'card_group, size: {len(card_group)} top3:{card_group[:3]}')
                    for card in card_group:
                        users = card.get('users')
                        if users:
                            for user in users:
                                code = str(user.get('id'))
                                followers_count = user.get('followers_count')
                                logger.debug(f'code:{code}')
                                if code and len(followers_count) > 1 and code not in user_ids:
                                    logger.info(f'add new uid:{code}')
                                    # user_ids.append(code)
                                    with open(save_file, 'a') as f:
                                        f.write(code + '\n')
            # sleep 1s
            time.sleep(1)
    except Exception as e:
        logger.error(f'error:{e}')


if __name__ == '__main__':
    user_ids = []

    f = open('famous_uid.txt')
    for line in f.readlines():
        user_ids.append(line.strip())
    logger.info(f'user_ids size:{len(user_ids)}')

    save_file = 'famous_uid_new.txt'
    # 基于已有的名人uid，爬取更多的名人uid（名人关注的一般都是名人）
    crawl_more_famous_uid(user_ids, save_file)
