# -*- coding: utf-8 -*-
"""
@author:XuMing(xuming624@qq.com)
@description:
"""

import codecs
import copy
import json
import math
import os
import random
import re
import sqlite3
import sys
from collections import OrderedDict
import csv
from datetime import date, datetime, timedelta
from pathlib import Path
from time import sleep
from requests.adapters import HTTPAdapter
from tqdm import tqdm
import requests
from lxml import etree
from loguru import logger
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
pwd_path = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(pwd_path, "weibo_data")

weibo_cookie = os.getenv("WEIBO_COOKIE", "")

# 日期时间格式
DTFORMAT = "%Y-%m-%d %H:%M:%S"

"""
运行模式
可以是追加模式append或覆盖模式overwrite
append模式：仅可在sqlite启用时使用。每次运行每个id只获取最新的微博，对于以往的即使是编辑过的微博，也不再获取。
overwrite模式：每次运行都会获取全量微博。
注意：overwrite模式下暂不能记录上次获取微博的id，因此从overwrite模式转为append模式时，仍需获取所有数据
"""
MODE = "append"

"""
检查cookie是否有效
默认不需要检查cookie
如果检查cookie，需要参考以下链接设置
config中science_date一定要确保测试号获得的微博数在不含测试微博的情况下大于9
https://github.com/dataabc/weibo-crawler#%E5%A6%82%E4%BD%95%E6%A3%80%E6%B5%8Bcookie%E6%98%AF%E5%90%A6%E6%9C%89%E6%95%88%E5%8F%AF%E9%80%89
"""
CHECK_COOKIE = {
    "CHECK": False,  # 是否检查cookie
    "CHECKED": False,  # 这里不要动，判断已检查了cookie的标志位
    "EXIT_AFTER_CHECK": False,  # 这里不要动，append模式中已完成增量微博抓取，仅等待cookie检查的标志位
    "HIDDEN_WEIBO": "微博内容",  # 你可能发现平台会自动给你的微博自动加个空格，但这里你不用加空格
    "GUESS_PIN": False,  # 这里不要动，因为微博取消了“置顶”字样的显示，因此默认猜测所有人第一条都是置顶
}


class WeiboCrawler:
    def __init__(self, user_id_list):
        """Weibo类初始化"""
        config = {
            "user_id_list": user_id_list,
            "only_crawl_original": 0,  # 0: 爬取所有微博，1: 仅爬取原创微博
            "since_date": 365,  # 爬取微博的时间范围，单位为天
            "start_page": 1,
            "write_mode": ["sqlite", "json"],
            "original_pic_download": 0,
            "retweet_pic_download": 0,
            "original_video_download": 0,
            "retweet_video_download": 0,
            "download_comment": 0,
            "comment_max_download_count": 50,
            "download_repost": 0,
            "repost_max_download_count": 20,
            "user_id_as_folder_name": 0,
            "remove_html_tag": 1
        }
        self.validate_config(config)
        self.screen_names = []  # 新增列
        self.only_crawl_original = config["only_crawl_original"]  # 取值范围为0、1,程序默认值为0,代表要爬取用户的全部微博,1代表只爬取用户的原创微博
        self.remove_html_tag = config[
            "remove_html_tag"
        ]  # 取值范围为0、1, 0代表不移除微博中的html tag, 1代表移除
        since_date = config["since_date"]
        # since_date 若为整数，则取该天数之前的日期；若为 yyyy-mm-dd，则增加时间
        if isinstance(since_date, int):
            since_date = date.today() - timedelta(since_date)
            since_date = since_date.strftime(DTFORMAT)
        elif self.is_date(since_date):
            since_date = "{} 00:00:00".format(since_date)
        elif self.is_datetime(since_date):
            pass
        self.since_date = since_date  # 起始时间，即爬取发布日期从该值到现在的微博，形式为yyyy-mm-ddThh:mm:ss，如：2023-08-21T09:23:03
        self.start_page = config.get("start_page", 1)  # 开始爬的页，如果中途被限制而结束可以用此定义开始页码
        self.write_mode = config[
            "write_mode"
        ]  # 结果信息保存类型，为list形式，可包含csv、json、sqlite
        self.original_pic_download = config[
            "original_pic_download"
        ]  # 取值范围为0、1, 0代表不下载原创微博图片,1代表下载
        self.retweet_pic_download = config[
            "retweet_pic_download"
        ]  # 取值范围为0、1, 0代表不下载转发微博图片,1代表下载
        self.original_video_download = config[
            "original_video_download"
        ]  # 取值范围为0、1, 0代表不下载原创微博视频,1代表下载
        self.retweet_video_download = config[
            "retweet_video_download"
        ]  # 取值范围为0、1, 0代表不下载转发微博视频,1代表下载
        self.download_comment = config["download_comment"]  # 1代表下载评论,0代表不下载
        self.comment_max_download_count = config[
            "comment_max_download_count"
        ]  # 如果设置了下评论，每条微博评论数会限制在这个值内
        self.download_repost = config["download_repost"]  # 1代表下载转发,0代表不下载
        self.repost_max_download_count = config[
            "repost_max_download_count"
        ]  # 如果设置了下转发，每条微博转发数会限制在这个值内
        self.user_id_as_folder_name = config.get(
            "user_id_as_folder_name", 0
        )  # 结果目录名，取值为0或1，决定结果文件存储在用户昵称文件夹里还是用户id文件夹里
        cookie = config.get("cookie")  # 微博cookie，可填可不填
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"
        self.headers = {"User_Agent": user_agent, "Cookie": cookie}
        self.post_config = config.get("post_config")  # post_config，可以不填
        self.max_page = config.get("max_page", 30)  # 爬取的最大页数
        user_id_list = config["user_id_list"]
        # 避免卡住
        if isinstance(user_id_list, list):
            random.shuffle(user_id_list)

        query_list = config.get("query_list") or []
        if isinstance(query_list, str):
            query_list = query_list.split(",")
        self.query_list = query_list
        if not isinstance(user_id_list, list):
            if not os.path.isabs(user_id_list):
                user_id_list = pwd_path + os.sep + user_id_list
            self.user_config_file_path = user_id_list  # 用户配置文件路径
            user_config_list = self.get_user_config_list(user_id_list)
        else:
            self.user_config_file_path = ""
            user_config_list = [
                {
                    "user_id": user_id,
                    "since_date": self.since_date,
                    "query_list": query_list,
                }
                for user_id in user_id_list
            ]

        self.user_config_list = user_config_list  # 要爬取的微博用户的user_config列表
        self.user_config = {}  # 用户配置,包含用户id和since_date
        self.start_date = ""  # 获取用户第一条微博时的日期
        self.query = ""
        self.user = {}  # 存储目标微博用户信息
        self.got_count = 0  # 存储爬取到的微博数
        self.weibo = []  # 存储爬取到的所有微博信息
        self.weibo_id_list = []  # 存储爬取到的所有微博id
        self.long_sleep_count_before_each_user = 0  # 每个用户前的长时间sleep避免被ban

    def validate_config(self, config):
        """验证配置是否正确"""

        # 验证如下1/0相关值
        argument_list = [
            "only_crawl_original",
            "original_pic_download",
            "retweet_pic_download",
            "original_video_download",
            "retweet_video_download",
            "download_comment",
            "download_repost",
        ]
        for argument in argument_list:
            if config[argument] != 0 and config[argument] != 1:
                logger.warning("%s值应为0或1,请重新输入", config[argument])
                sys.exit()

        # 验证query_list
        query_list = config.get("query_list") or []
        if (not isinstance(query_list, list)) and (not isinstance(query_list, str)):
            logger.warning("query_list值应为list类型或字符串,请重新输入")
            sys.exit()

        # 验证write_mode
        write_mode = ["csv", "json", "sqlite"]
        if not isinstance(config["write_mode"], list):
            sys.exit("write_mode值应为list类型")
        for mode in config["write_mode"]:
            if mode not in write_mode:
                logger.warning(f" {mode} 为无效模式，请从 {write_mode} 中挑选一个或多个作为write_mode")
                sys.exit()
        # 验证运行模式
        if "sqlite" not in config["write_mode"] and MODE == "append":
            logger.warning("append模式下请将sqlite加入write_mode中")
            sys.exit()

        # 验证user_id_list
        user_id_list = config["user_id_list"]
        if (not isinstance(user_id_list, list)) and (not user_id_list.endswith(".txt")):
            logger.warning("user_id_list值应为list类型或txt文件路径")
            sys.exit()
        if not isinstance(user_id_list, list):
            if not os.path.isabs(user_id_list):
                user_id_list = pwd_path + os.sep + user_id_list
            if not os.path.isfile(user_id_list):
                logger.warning(f"不存在{user_id_list}文件")
                sys.exit()

        # 验证since_date
        since_date = config["since_date"]
        if (not isinstance(since_date, int)) and (not self.is_datetime(since_date)) and (not self.is_date(since_date)):
            logger.warning("since_date值应为yyyy-mm-dd形式、yyyy-mm-ddTHH:MM:SS形式或整数，请重新输入")
            sys.exit()

        comment_max_count = config["comment_max_download_count"]
        if not isinstance(comment_max_count, int):
            logger.warning("最大下载评论数 (comment_max_download_count) 应为整数类型")
            sys.exit()
        elif comment_max_count < 0:
            logger.warning("最大下载评论数 (comment_max_download_count) 应该为正整数")
            sys.exit()

        repost_max_count = config["repost_max_download_count"]
        if not isinstance(repost_max_count, int):
            logger.warning("最大下载转发数 (repost_max_download_count) 应为整数类型")
            sys.exit()
        elif repost_max_count < 0:
            logger.warning("最大下载转发数 (repost_max_download_count) 应该为正整数")
            sys.exit()

    def is_datetime(self, since_date):
        """判断日期格式是否为 %Y-%m-%dT%H:%M:%S"""
        try:
            datetime.strptime(since_date, DTFORMAT)
            return True
        except ValueError:
            return False

    def is_date(self, since_date):
        """判断日期格式是否为 %Y-%m-%d"""
        try:
            datetime.strptime(since_date, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def get_json(self, params):
        """获取网页中json数据"""
        url = "https://m.weibo.cn/api/container/getIndex?"
        r = requests.get(url, params=params, headers=self.headers, verify=False)
        return r.json(), r.status_code

    def get_weibo_json(self, page):
        """获取网页中微博json数据"""
        params = (
            {
                "container_ext": "profile_uid:" + str(self.user_config["user_id"]),
                "containerid": "100103type=401&q=" + self.query,
                "page_type": "searchall",
            }
            if self.query
            else {"containerid": "230413" + str(self.user_config["user_id"])}
        )
        params["page"] = page
        js, _ = self.get_json(params)
        return js

    def user_to_csv(self):
        """将爬取到的用户信息写入csv文件"""
        os.makedirs(DATA_DIR, exist_ok=True)
        file_path = DATA_DIR + os.sep + "users.csv"
        self.user_csv_file_path = file_path
        result_headers = [
            "用户id",
            "昵称",
            "性别",
            "生日",
            "所在地",
            "学习经历",
            "公司",
            "注册时间",
            "阳光信用",
            "微博数",
            "粉丝数",
            "关注数",
            "简介",
            "主页",
            "头像",
            "高清头像",
            "微博等级",
            "会员等级",
            "是否认证",
            "认证类型",
            "认证信息",
            "上次记录微博信息",
        ]
        result_data = [
            [
                v.encode("utf-8") if "unicode" in str(type(v)) else v
                for v in self.user.values()
            ]
        ]
        # 已经插入信息的用户无需重复插入，返回的id是空字符串或微博id 发布日期%Y-%m-%d
        last_weibo_msg = insert_or_update_user(
            logger, result_headers, result_data, file_path
        )
        self.last_weibo_id = last_weibo_msg.split(" ")[0] if last_weibo_msg else ""
        self.last_weibo_date = (
            last_weibo_msg.split(" ")[1]
            if last_weibo_msg
            else self.user_config["since_date"]
        )
        logger.info(
            f"last_weibo_msg: {last_weibo_msg}, "
            f"last_weibo_id: {self.last_weibo_id}, "
            f"last_weibo_date: {self.last_weibo_date}"
        )

    def user_to_database(self):
        """将用户信息写入文件/数据库"""
        self.user_to_csv()
        if "sqlite" in self.write_mode:
            self.user_to_sqlite()

    def get_user_info(self):
        """获取用户信息"""
        params = {"containerid": "100505" + str(self.user_config["user_id"])}

        # 这里在读取下一个用户的时候很容易被ban，需要优化休眠时长
        # 加一个count，不需要一上来啥都没干就sleep
        if self.long_sleep_count_before_each_user > 0:
            sleep_time = random.randint(1, 30)
            # 添加log，否则一般用户不知道以为程序卡了
            logger.info(f"""短暂sleep {sleep_time}秒，避免被ban""")
            sleep(sleep_time)
            logger.info("sleep结束")
        self.long_sleep_count_before_each_user = self.long_sleep_count_before_each_user + 1

        js, status_code = self.get_json(params)
        if status_code != 200:
            logger.info("被ban了，需要等待一段时间")
            sys.exit()
        if js["ok"]:
            info = js["data"]["userInfo"]
            user_info = OrderedDict()
            user_info["id"] = self.user_config["user_id"]
            user_info["screen_name"] = info.get("screen_name", "")
            user_info["gender"] = info.get("gender", "")
            params = {
                "containerid": "230283" + str(self.user_config["user_id"]) + "_-_INFO"
            }
            zh_list = ["生日", "所在地", "小学", "初中", "高中", "大学", "公司", "注册时间", "阳光信用"]
            en_list = [
                "birthday",
                "location",
                "education",
                "education",
                "education",
                "education",
                "company",
                "registration_time",
                "sunshine",
            ]
            for i in en_list:
                user_info[i] = ""
            js, _ = self.get_json(params)
            if js["ok"]:
                cards = js["data"]["cards"]
                if isinstance(cards, list) and len(cards) > 1:
                    card_list = cards[0]["card_group"] + cards[1]["card_group"]
                    for card in card_list:
                        if card.get("item_name") in zh_list:
                            user_info[
                                en_list[zh_list.index(card.get("item_name"))]
                            ] = card.get("item_content", "")
            user_info["statuses_count"] = self.string_to_int(
                info.get("statuses_count", 0)
            )
            user_info["followers_count"] = self.string_to_int(
                info.get("followers_count", 0)
            )
            user_info["follow_count"] = self.string_to_int(info.get("follow_count", 0))
            user_info["description"] = info.get("description", "")
            user_info["profile_url"] = info.get("profile_url", "")
            user_info["profile_image_url"] = info.get("profile_image_url", "")
            user_info["avatar_hd"] = info.get("avatar_hd", "")
            user_info["urank"] = info.get("urank", 0)
            user_info["mbrank"] = info.get("mbrank", 0)
            user_info["verified"] = info.get("verified", False)
            user_info["verified_type"] = info.get("verified_type", -1)
            user_info["verified_reason"] = info.get("verified_reason", "")
            user = self.standardize_info(user_info)
            self.user = user
            self.user_to_database()
            return 0
        else:
            logger.info("user_id_list中 {} id出错".format(self.user_config["user_id"]))
            return -1

    def get_long_weibo(self, id):
        """获取长微博"""
        for i in range(5):
            url = "https://m.weibo.cn/detail/%s" % id
            logger.info(f"""URL: {url} """)
            html = requests.get(url, headers=self.headers, verify=False).text
            html = html[html.find('"status":'):]
            html = html[: html.rfind('"call"')]
            html = html[: html.rfind(",")]
            html = "{" + html + "}"
            js = json.loads(html, strict=False)
            weibo_info = js.get("status")
            if weibo_info:
                weibo = self.parse_weibo(weibo_info)
                return weibo
            sleep(random.randint(6, 10))

    def get_pics(self, weibo_info):
        """获取微博原始图片url"""
        if weibo_info.get("pics"):
            pic_info = weibo_info["pics"]
            pic_list = [pic["large"]["url"] for pic in pic_info]
            pics = ",".join(pic_list)
        else:
            pics = ""
        return pics

    def get_live_photo(self, weibo_info):
        """获取live photo中的视频url"""
        live_photo_list = weibo_info.get("live_photo", [])
        return live_photo_list

    def get_video_url(self, weibo_info):
        """获取微博视频url"""
        video_url = ""
        video_url_list = []
        if weibo_info.get("page_info"):
            if (
                    weibo_info["page_info"].get("urls")
                    or weibo_info["page_info"].get("media_info")
            ) and weibo_info["page_info"].get("type") == "video":
                media_info = weibo_info["page_info"]["urls"]
                if not media_info:
                    media_info = weibo_info["page_info"]["media_info"]
                video_url = media_info.get("mp4_720p_mp4")
                if not video_url:
                    video_url = media_info.get("mp4_hd_url")
                if not video_url:
                    video_url = media_info.get("hevc_mp4_hd")
                if not video_url:
                    video_url = media_info.get("mp4_sd_url")
                if not video_url:
                    video_url = media_info.get("mp4_ld_mp4")
                if not video_url:
                    video_url = media_info.get("stream_url_hd")
                if not video_url:
                    video_url = media_info.get("stream_url")
        if video_url:
            video_url_list.append(video_url)
        live_photo_list = self.get_live_photo(weibo_info)
        if live_photo_list:
            video_url_list += live_photo_list
        return ";".join(video_url_list)

    def download_one_file(self, url, file_path, type, weibo_id):
        """下载单个文件(图片/视频)"""
        try:

            file_exist = os.path.isfile(file_path)
            need_download = (not file_exist)
            sqlite_exist = False
            if "sqlite" in self.write_mode:
                sqlite_exist = self.sqlite_exist_file(file_path)
                if not sqlite_exist:
                    need_download = True

            if not need_download:
                return

            s = requests.Session()
            s.mount(url, HTTPAdapter(max_retries=5))
            try_count = 0
            success = False
            MAX_TRY_COUNT = 3
            while try_count < MAX_TRY_COUNT:
                downloaded = s.get(
                    url, headers=self.headers, timeout=(5, 10), verify=False
                )
                try_count += 1
                fail_flg_1 = url.endswith(("jpg", "jpeg")) and not downloaded.content.endswith(b"\xff\xd9")
                fail_flg_2 = url.endswith("png") and not downloaded.content.endswith(b"\xaeB`\x82")

                if (fail_flg_1 or fail_flg_2):
                    logger.debug("[DEBUG] failed " + url + "  " + str(try_count))
                else:
                    success = True
                    logger.debug("[DEBUG] success " + url + "  " + str(try_count))
                    break

            if success:
                # 需要分别判断是否需要下载
                if not file_exist:
                    with open(file_path, "wb") as f:
                        f.write(downloaded.content)
                        logger.debug("saved: " + file_path)
                if (not sqlite_exist) and ("sqlite" in self.write_mode):
                    self.insert_file_sqlite(
                        file_path, weibo_id, url, downloaded.content
                    )
            else:
                logger.debug("[DEBUG] failed " + url + " TOTALLY")
        except Exception as e:
            error_file = self.get_filepath(type) + os.sep + "not_downloaded.txt"
            with open(error_file, "ab") as f:
                url = str(weibo_id) + ":" + file_path + ":" + url + "\n"
                f.write(url.encode(sys.stdout.encoding))
            logger.exception(e)

    def sqlite_exist_file(self, url):
        if not os.path.exists(self.get_sqlte_path()):
            return True
        con = self.get_sqlite_connection()
        cur = con.cursor()

        query_sql = """SELECT url FROM bins WHERE path=? """
        count = cur.execute(query_sql, (url,)).fetchone()
        con.close()
        if count is None:
            return False

        return True

    def insert_file_sqlite(self, file_path, weibo_id, url, binary):
        if not weibo_id:
            return
        extension = Path(file_path).suffix
        if not extension:
            return
        if len(binary) <= 0:
            return

        file_data = OrderedDict()
        file_data["weibo_id"] = weibo_id
        file_data["ext"] = extension
        file_data["data"] = binary
        file_data["path"] = file_path
        file_data["url"] = url

        con = self.get_sqlite_connection()
        self.sqlite_insert(con, file_data, "bins")
        con.close()

    def handle_download(self, file_type, file_dir, urls, w):
        """处理下载相关操作"""
        file_prefix = w["created_at"][:11].replace("-", "") + "_" + str(w["id"])
        if file_type == "img":
            if "," in urls:
                url_list = urls.split(",")
                for i, url in enumerate(url_list):
                    index = url.rfind(".")
                    if len(url) - index >= 5:
                        file_suffix = ".jpg"
                    else:
                        file_suffix = url[index:]
                    file_name = file_prefix + "_" + str(i + 1) + file_suffix
                    file_path = file_dir + os.sep + file_name
                    self.download_one_file(url, file_path, file_type, w["id"])
            else:
                index = urls.rfind(".")
                if len(urls) - index > 5:
                    file_suffix = ".jpg"
                else:
                    file_suffix = urls[index:]
                file_name = file_prefix + file_suffix
                file_path = file_dir + os.sep + file_name
                self.download_one_file(urls, file_path, file_type, w["id"])
        else:
            file_suffix = ".mp4"
            if ";" in urls:
                url_list = urls.split(";")
                if url_list[0].endswith(".mov"):
                    file_suffix = ".mov"
                for i, url in enumerate(url_list):
                    file_name = file_prefix + "_" + str(i + 1) + file_suffix
                    file_path = file_dir + os.sep + file_name
                    self.download_one_file(url, file_path, file_type, w["id"])
            else:
                if urls.endswith(".mov"):
                    file_suffix = ".mov"
                file_name = file_prefix + file_suffix
                file_path = file_dir + os.sep + file_name
                self.download_one_file(urls, file_path, file_type, w["id"])

    def download_files(self, file_type, weibo_type, wrote_count):
        """下载文件(图片/视频)"""
        try:
            describe = ""
            if file_type == "img":
                describe = "图片"
                key = "pics"
            else:
                describe = "视频"
                key = "video_url"
            if weibo_type == "original":
                describe = "原创微博" + describe
            else:
                describe = "转发微博" + describe
            logger.info("即将进行%s下载", describe)
            file_dir = self.get_filepath(file_type)
            file_dir = file_dir + os.sep + describe
            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)
            for w in tqdm(self.weibo[wrote_count:], desc="Download progress"):
                if weibo_type == "retweet":
                    if w.get("retweet"):
                        w = w["retweet"]
                    else:
                        continue
                if w.get(key):
                    self.handle_download(file_type, file_dir, w.get(key), w)
            logger.info("%s下载完毕,保存路径:", describe)
            logger.info(file_dir)
        except Exception as e:
            logger.exception(e)

    def get_location(self, selector):
        """获取微博发布位置"""
        location_icon = "timeline_card_small_location_default.png"
        span_list = selector.xpath("//span")
        location = ""
        for i, span in enumerate(span_list):
            if span.xpath("img/@src"):
                if location_icon in span.xpath("img/@src")[0]:
                    location = span_list[i + 1].xpath("string(.)")
                    break
        return location

    def get_article_url(self, selector):
        """获取微博中头条文章的url"""
        article_url = ""
        text = selector.xpath("string(.)")
        if text.startswith("发布了头条文章"):
            url = selector.xpath("//a/@data-url")
            if url and url[0].startswith("http://t.cn"):
                article_url = url[0]
        return article_url

    def get_topics(self, selector):
        """获取参与的微博话题"""
        span_list = selector.xpath("//span[@class='surl-text']")
        topics = ""
        topic_list = []
        for span in span_list:
            text = span.xpath("string(.)")
            if len(text) > 2 and text[0] == "#" and text[-1] == "#":
                topic_list.append(text[1:-1])
        if topic_list:
            topics = ",".join(topic_list)
        return topics

    def get_at_users(self, selector):
        """获取@用户"""
        a_list = selector.xpath("//a")
        at_users = ""
        at_list = []
        for a in a_list:
            if "@" + a.xpath("@href")[0][3:] == a.xpath("string(.)"):
                at_list.append(a.xpath("string(.)")[1:])
        if at_list:
            at_users = ",".join(at_list)
        return at_users

    def string_to_int(self, string):
        """字符串转换为整数"""
        if isinstance(string, int):
            return string
        elif string.endswith("万+"):
            string = string[:-2] + "0000"
        elif string.endswith("万"):
            string = float(string[:-1]) * 10000
        elif string.endswith("亿"):
            string = float(string[:-1]) * 100000000
        return int(string)

    def standardize_date(self, created_at):
        """标准化微博发布时间"""
        if "刚刚" in created_at:
            ts = datetime.now()
        elif "分钟" in created_at:
            minute = created_at[: created_at.find("分钟")]
            minute = timedelta(minutes=int(minute))
            ts = datetime.now() - minute
        elif "小时" in created_at:
            hour = created_at[: created_at.find("小时")]
            hour = timedelta(hours=int(hour))
            ts = datetime.now() - hour
        elif "昨天" in created_at:
            day = timedelta(days=1)
            ts = datetime.now() - day
        else:
            created_at = created_at.replace("+0800 ", "")
            ts = datetime.strptime(created_at, "%c")

        created_at = ts.strftime(DTFORMAT)
        full_created_at = ts.strftime("%Y-%m-%d %H:%M:%S")
        return created_at, full_created_at

    def standardize_info(self, weibo):
        """标准化信息，去除乱码"""
        for k, v in weibo.items():
            if (
                    "bool" not in str(type(v))
                    and "int" not in str(type(v))
                    and "list" not in str(type(v))
                    and "long" not in str(type(v))
            ):
                weibo[k] = (
                    v.replace("\u200b", "")
                    .encode(sys.stdout.encoding, "ignore")
                    .decode(sys.stdout.encoding)
                )
        return weibo

    def parse_weibo(self, weibo_info):
        weibo = OrderedDict()
        if weibo_info["user"]:
            weibo["user_id"] = weibo_info["user"]["id"]
            weibo["screen_name"] = weibo_info["user"]["screen_name"]
        else:
            weibo["user_id"] = ""
            weibo["screen_name"] = ""
        weibo["id"] = int(weibo_info["id"])
        weibo["bid"] = weibo_info["bid"]
        text_body = weibo_info["text"]
        selector = etree.HTML(f"{text_body}<hr>" if text_body.isspace() else text_body)
        if self.remove_html_tag:
            text_list = selector.xpath("//text()")
            # 若text_list中的某个字符串元素以 @ 或 # 开始，则将该元素与前后元素合并为新元素，否则会带来没有必要的换行
            text_list_modified = []
            for ele in range(len(text_list)):
                if ele > 0 and (text_list[ele - 1].startswith(('@', '#')) or text_list[ele].startswith(('@', '#'))):
                    text_list_modified[-1] += text_list[ele]
                else:
                    text_list_modified.append(text_list[ele])
            weibo["text"] = "\n".join(text_list_modified)
        else:
            weibo["text"] = text_body
        weibo["article_url"] = self.get_article_url(selector)
        weibo["pics"] = self.get_pics(weibo_info)
        weibo["video_url"] = self.get_video_url(weibo_info)
        weibo["location"] = self.get_location(selector)
        weibo["created_at"] = weibo_info["created_at"]
        weibo["source"] = weibo_info["source"]
        weibo["attitudes_count"] = self.string_to_int(
            weibo_info.get("attitudes_count", 0)
        )
        weibo["comments_count"] = self.string_to_int(
            weibo_info.get("comments_count", 0)
        )
        weibo["reposts_count"] = self.string_to_int(weibo_info.get("reposts_count", 0))
        weibo["topics"] = self.get_topics(selector)
        weibo["at_users"] = self.get_at_users(selector)
        return self.standardize_info(weibo)

    def print_user_info(self):
        """打印用户信息"""
        logger.info("+" * 100)
        logger.info("用户信息")
        logger.info("用户id：%s", self.user["id"])
        logger.info("用户昵称：%s", self.user["screen_name"])
        gender = "女" if self.user["gender"] == "f" else "男"
        logger.info("性别：%s", gender)
        logger.info("生日：%s", self.user["birthday"])
        logger.info("所在地：%s", self.user["location"])
        logger.info("教育经历：%s", self.user["education"])
        logger.info("公司：%s", self.user["company"])
        logger.info("阳光信用：%s", self.user["sunshine"])
        logger.info("注册时间：%s", self.user["registration_time"])
        logger.info("微博数：%d", self.user["statuses_count"])
        logger.info("粉丝数：%d", self.user["followers_count"])
        logger.info("关注数：%d", self.user["follow_count"])
        logger.info("url：https://m.weibo.cn/profile/%s", self.user["id"])
        if self.user.get("verified_reason"):
            logger.info(self.user["verified_reason"])
        logger.info(self.user["description"])
        logger.info("+" * 100)

    def print_one_weibo(self, weibo):
        """打印一条微博"""
        try:
            logger.info("微博id：%d", weibo["id"])
            logger.info("微博正文：%s", weibo["text"])
            logger.info("原始图片url：%s", weibo["pics"])
            logger.info("微博位置：%s", weibo["location"])
            logger.info("发布时间：%s", weibo["created_at"])
            logger.info("发布工具：%s", weibo["source"])
            logger.info("点赞数：%d", weibo["attitudes_count"])
            logger.info("评论数：%d", weibo["comments_count"])
            logger.info("转发数：%d", weibo["reposts_count"])
            logger.info("话题：%s", weibo["topics"])
            logger.info("@用户：%s", weibo["at_users"])
            logger.info("url：https://m.weibo.cn/detail/%d", weibo["id"])
        except OSError:
            pass

    def print_weibo(self, weibo):
        """打印微博，若为转发微博，会同时打印原创和转发部分"""
        if weibo.get("retweet"):
            logger.info("*" * 100)
            logger.info("转发部分：")
            self.print_one_weibo(weibo["retweet"])
            logger.info("*" * 100)
            logger.info("原创部分：")
        self.print_one_weibo(weibo)
        logger.info("-" * 120)

    def get_one_weibo(self, info):
        """获取一条微博的全部信息"""
        try:
            weibo_info = info["mblog"]
            weibo_id = weibo_info["id"]
            retweeted_status = weibo_info.get("retweeted_status")
            is_long = (
                True if weibo_info.get("pic_num") > 9 else weibo_info.get("isLongText")
            )
            if retweeted_status and retweeted_status.get("id"):  # 转发
                retweet_id = retweeted_status.get("id")
                is_long_retweet = retweeted_status.get("isLongText")
                if is_long:
                    weibo = self.get_long_weibo(weibo_id)
                    if not weibo:
                        weibo = self.parse_weibo(weibo_info)
                else:
                    weibo = self.parse_weibo(weibo_info)
                if is_long_retweet:
                    retweet = self.get_long_weibo(retweet_id)
                    if not retweet:
                        retweet = self.parse_weibo(retweeted_status)
                else:
                    retweet = self.parse_weibo(retweeted_status)
                (
                    retweet["created_at"],
                    retweet["full_created_at"],
                ) = self.standardize_date(retweeted_status["created_at"])
                weibo["retweet"] = retweet
            else:  # 原创
                if is_long:
                    weibo = self.get_long_weibo(weibo_id)
                    if not weibo:
                        weibo = self.parse_weibo(weibo_info)
                else:
                    weibo = self.parse_weibo(weibo_info)
            weibo["created_at"], weibo["full_created_at"] = self.standardize_date(weibo_info["created_at"])
            return weibo
        except Exception as e:
            logger.exception(e)

    def get_weibo_comments(self, weibo, max_count, on_downloaded):
        """
        Get weibo comments
        :weibo standardlized weibo
        :max_count 最大允许下载数
        :on_downloaded 下载完成时的实例方法回调
        """
        if weibo["comments_count"] == 0:
            return

        logger.debug("正在下载评论 微博id:{id}".format(id=weibo["id"]))
        self._get_weibo_comments_cookie(weibo, 0, max_count, None, on_downloaded)

    def get_weibo_reposts(self, weibo, max_count, on_downloaded):
        """
        :weibo standardlized weibo
        :max_count 最大允许下载数
        :on_downloaded 下载完成时的实例方法回调
        """
        if weibo["reposts_count"] == 0:
            return

        logger.info("正在下载转发 微博id:{id}".format(id=weibo["id"]))
        self._get_weibo_reposts_cookie(weibo, 0, max_count, 1, on_downloaded)

    def _get_weibo_comments_cookie(
            self, weibo, cur_count, max_count, max_id, on_downloaded
    ):
        """
        :weibo standardlized weibo
        :cur_count  已经下载的评论数
        :max_count 最大允许下载数
        :max_id 微博返回的max_id参数
        :on_downloaded 下载完成时的实例方法回调
        """
        if cur_count >= max_count:
            return

        id = weibo["id"]
        params = {"mid": id}
        if max_id:
            params["max_id"] = max_id
        url = "https://m.weibo.cn/comments/hotflow?max_id_type=0"

        json_res = None
        error = False
        try:
            req = requests.get(
                url,
                params=params,
                headers=self.headers,
            )
            json_res = req.json()
        except Exception as e:
            # 没有cookie会抓取失败
            # 微博日期小于某个日期的用这个url会被403 需要用老办法尝试一下
            error = True

        if error:
            # 最大好像只能有50条 TODO: improvement
            self._get_weibo_comments_nocookie(weibo, 0, max_count, 1, on_downloaded)
            return

        data = json_res.get("data")
        if not data:
            # 新接口没有抓取到的老接口也试一下
            self._get_weibo_comments_nocookie(weibo, 0, max_count, 1, on_downloaded)
            return

        comments = data.get("data")
        count = len(comments)
        if count == 0:
            # 没有了可以直接跳出递归
            return

        if on_downloaded:
            on_downloaded(weibo, comments)

        # 随机睡眠一下
        if max_count % 40 == 0:
            sleep(random.randint(1, 5))

        cur_count += count
        max_id = data.get("max_id")

        if max_id == 0:
            return

        self._get_weibo_comments_cookie(
            weibo, cur_count, max_count, max_id, on_downloaded
        )

    def _get_weibo_comments_nocookie(
            self, weibo, cur_count, max_count, page, on_downloaded
    ):
        """
        :weibo standardlized weibo
        :cur_count  已经下载的评论数
        :max_count 最大允许下载数
        :page 下载的页码 从 1 开始
        :on_downloaded 下载完成时的实例方法回调
        """
        if cur_count >= max_count:
            return
        id = weibo["id"]
        url = "https://m.weibo.cn/api/comments/show?id={id}&page={page}".format(
            id=id, page=page
        )

        try:
            req = requests.get(url)
            json_res = req.json()
        except Exception as e:
            logger.warning("未能抓取完整评论 微博id: {}, error: {}".format(id, str(e)))
            return

        data = json_res.get("data")
        if not data:
            return
        comments = data.get("data")
        count = len(comments)
        if count == 0:
            # 没有了可以直接跳出递归
            return

        if on_downloaded:
            on_downloaded(weibo, comments)

        cur_count += count
        page += 1

        # 随机睡眠一下
        if page % 2 == 0:
            sleep(random.randint(1, 5))

        req_page = data.get("max")

        if req_page == 0:
            return

        if page > req_page:
            return
        self._get_weibo_comments_nocookie(
            weibo, cur_count, max_count, page, on_downloaded
        )

    def _get_weibo_reposts_cookie(
            self, weibo, cur_count, max_count, page, on_downloaded
    ):
        """
        :weibo standardlized weibo
        :cur_count  已经下载的转发数
        :max_count 最大允许下载数
        :page 下载的页码 从 1 开始
        :on_downloaded 下载完成时的实例方法回调
        """
        if cur_count >= max_count:
            return
        id = weibo["id"]
        url = "https://m.weibo.cn/api/statuses/repostTimeline"
        params = {"id": id, "page": page}
        req = requests.get(
            url,
            params=params,
            headers=self.headers,
        )

        json_res = None
        try:
            json_res = req.json()
        except Exception as e:
            logger.warning("未能抓取完整转发 微博id: {}, error: {}".format(id, str(e)))
            return

        data = json_res.get("data")
        if not data:
            return
        reposts = data.get("data")
        count = len(reposts)
        if count == 0:
            # 没有了可以直接跳出递归
            return

        if on_downloaded:
            on_downloaded(weibo, reposts)

        cur_count += count
        page += 1

        # 随机睡眠一下
        if page % 2 == 0:
            sleep(random.randint(2, 5))

        req_page = data.get("max")

        if req_page == 0:
            return

        if page > req_page:
            return
        self._get_weibo_reposts_cookie(weibo, cur_count, max_count, page, on_downloaded)

    def is_pinned_weibo(self, info):
        """判断微博是否为置顶微博"""
        weibo_info = info["mblog"]
        isTop = weibo_info.get("isTop")
        if isTop:
            return True
        else:
            return False

    def get_one_page(self, page):
        """获取一页的全部微博"""
        try:
            js = self.get_weibo_json(page)
            if js["ok"]:
                weibos = js["data"]["cards"]

                if self.query:
                    weibos = weibos[0]["card_group"]
                # 如果需要检查cookie，在循环第一个人的时候，就要看看仅自己可见的信息有没有，要是没有直接报错
                for w in weibos:
                    if w["card_type"] == 11:
                        temp = w.get("card_group", [0])
                        if len(temp) >= 1:
                            w = temp[0] or w
                        else:
                            w = w
                    if w["card_type"] == 9:
                        wb = self.get_one_weibo(w)
                        if wb:
                            if (
                                    CHECK_COOKIE["CHECK"]
                                    and (not CHECK_COOKIE["CHECKED"])
                                    and wb["text"].startswith(CHECK_COOKIE["HIDDEN_WEIBO"])
                            ):
                                CHECK_COOKIE["CHECKED"] = True
                                logger.info("cookie检查通过")
                                if CHECK_COOKIE["EXIT_AFTER_CHECK"]:
                                    return True
                            if wb["id"] in self.weibo_id_list:
                                continue
                            created_at = datetime.strptime(wb["created_at"], DTFORMAT)
                            since_date = datetime.strptime(
                                self.user_config["since_date"], DTFORMAT
                            )
                            if MODE == "append":
                                if CHECK_COOKIE["GUESS_PIN"]:
                                    CHECK_COOKIE["GUESS_PIN"] = False
                                    continue

                                if self.first_crawler:
                                    # 置顶微博的具体时间不好判定，将非置顶微博当成最新微博，写入上次抓取id的csv
                                    self.latest_weibo_id = str(wb["id"])
                                    update_last_weibo_id(
                                        wb["user_id"],
                                        str(wb["id"]) + " " + wb["created_at"],
                                        self.user_csv_file_path,
                                    )
                                    self.first_crawler = False
                                if str(wb["id"]) == self.last_weibo_id:
                                    if CHECK_COOKIE["CHECK"] and (not CHECK_COOKIE["CHECKED"]):
                                        # 已经爬取过最新的了，只是没检查到cookie，一旦检查通过，直接放行
                                        CHECK_COOKIE["EXIT_AFTER_CHECK"] = True
                                        continue
                                    if self.last_weibo_id == self.latest_weibo_id:
                                        logger.info("{} 用户没有发新微博".format(self.user["screen_name"]))
                                    else:
                                        logger.info(
                                            "增量获取微博完毕，将最新微博id从 {} 变更为 {}".format(self.last_weibo_id,
                                                                                                  self.latest_weibo_id))
                                    return True
                                # 上一次标记的微博被删了，就把上一条微博时间记录推前两天，多抓点评论或者微博内容修改
                                # TODO 更加合理的流程是，即使读取到上次更新微博id，也抓取增量评论，由此获得更多的评论
                                since_date = datetime.strptime(
                                    convert_to_days_ago(self.last_weibo_date, 1),
                                    DTFORMAT,
                                )
                            if created_at < since_date:
                                if self.is_pinned_weibo(w):
                                    continue
                                # 如果要检查还没有检查cookie，不能直接跳出
                                elif CHECK_COOKIE["CHECK"] and (
                                        not CHECK_COOKIE["CHECKED"]
                                ):
                                    continue
                                else:
                                    logger.info(
                                        "已获取{}({})的第{}页{}微博".format(
                                            self.user["screen_name"],
                                            self.user["id"],
                                            page,
                                            '包含"' + self.query + '"的'
                                            if self.query
                                            else "",
                                        )
                                    )
                                    return True
                            if (not self.only_crawl_original) or ("retweet" not in wb.keys()):
                                if wb.get("text"):
                                    self.weibo.append(wb)
                                    self.weibo_id_list.append(wb["id"])
                                    self.got_count += 1
                                    logger.debug(
                                        "已获取用户 {} 的微博，内容为 {}".format(self.user["screen_name"], wb["text"]))
                            else:
                                logger.debug("过滤[转发微博|无内容微博]")

                if CHECK_COOKIE["CHECK"] and not CHECK_COOKIE["CHECKED"]:
                    logger.warning("经检查，cookie无效，系统退出")
                    sys.exit()
            else:
                return True
        except Exception as e:
            logger.exception(e)

    def get_page_count(self):
        """获取微博页数"""
        try:
            weibo_count = self.user["statuses_count"]
            page_count = int(math.ceil(weibo_count / 10.0))
            return page_count
        except KeyError:
            logger.exception(
                "程序出错，错误原因可能为以下两者：\n"
                "1.user_id不正确；\n"
                "2.此用户微博可能需要设置cookie才能爬取。\n"
                "解决方案：\n"
                "请参考\n"
                "https://github.com/dataabc/weibo-crawler#如何获取user_id\n"
                "获取正确的user_id；\n"
                "或者参考\n"
                "https://github.com/dataabc/weibo-crawler#3程序设置\n"
                "中的“设置cookie”部分设置cookie信息"
            )

    def get_write_info(self, wrote_count):
        """获取要写入的微博信息"""
        write_info = []
        for w in self.weibo[wrote_count:]:
            wb = OrderedDict()
            for k, v in w.items():
                if k not in ["user_id", "screen_name", "retweet"]:
                    if "unicode" in str(type(v)):
                        v = v.encode("utf-8")
                    if k == "id":
                        v = str(v) + "\t"
                    wb[k] = v
            if not self.only_crawl_original:
                if w.get("retweet"):
                    wb["is_original"] = False
                    for k2, v2 in w["retweet"].items():
                        if "unicode" in str(type(v2)):
                            v2 = v2.encode("utf-8")
                        if k2 == "id":
                            v2 = str(v2) + "\t"
                        wb["retweet_" + k2] = v2
                else:
                    wb["is_original"] = True
            write_info.append(wb)
        return write_info

    def get_filepath(self, type):
        """获取结果文件路径"""
        try:
            dir_name = self.user["screen_name"]
            if self.user_id_as_folder_name:
                dir_name = str(self.user_config["user_id"])
            file_dir = DATA_DIR + os.sep + dir_name
            if type == "img" or type == "video":
                file_dir = file_dir + os.sep + type
            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)
            if type == "img" or type == "video":
                return file_dir
            file_path = file_dir + os.sep + str(self.user_config["user_id"]) + "." + type
            return file_path
        except Exception as e:
            logger.exception(e)

    def get_result_headers(self):
        """获取要写入结果文件的表头"""
        result_headers = [
            "id",
            "bid",
            "正文",
            "头条文章url",
            "原始图片url",
            "视频url",
            "位置",
            "日期",
            "工具",
            "点赞数",
            "评论数",
            "转发数",
            "话题",
            "@用户",
            "完整日期",
        ]
        if not self.only_crawl_original:
            result_headers2 = ["是否原创", "源用户id", "源用户昵称"]
            result_headers3 = ["源微博" + r for r in result_headers]
            result_headers = result_headers + result_headers2 + result_headers3
        return result_headers

    def write_csv(self, wrote_count):
        """将爬到的信息写入csv文件"""
        write_info = self.get_write_info(wrote_count)
        result_headers = self.get_result_headers()
        result_data = [w.values() for w in write_info]
        file_path = self.get_filepath("csv")
        self.csv_helper(result_headers, result_data, file_path)

    def csv_helper(self, headers, result_data, file_path):
        """将指定信息写入csv文件"""
        if not os.path.isfile(file_path):
            is_first_write = 1
        else:
            is_first_write = 0
        with open(file_path, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            if is_first_write:
                writer.writerows([headers])
            writer.writerows(result_data)
        if headers[0] == "id":
            logger.info(f"{self.got_count}条微博写入csv文件完毕,保存路径:{file_path}")
        else:
            logger.info(f'{self.user["screen_name"]} 信息写入csv文件完毕，保存路径:{file_path}')

    def update_json_data(self, data, weibo_info):
        """更新要写入json结果文件中的数据，已经存在于json中的信息更新为最新值，不存在的信息添加到data中"""
        data["user"] = self.user
        if data.get("weibo"):
            is_new = 1  # 待写入微博是否全部为新微博，即待写入微博与json中的数据不重复
            for old in data["weibo"]:
                if weibo_info[-1]["id"] == old["id"]:
                    is_new = 0
                    break
            if is_new == 0:
                for new in weibo_info:
                    flag = 1
                    for i, old in enumerate(data["weibo"]):
                        if new["id"] == old["id"]:
                            data["weibo"][i] = new
                            flag = 0
                            break
                    if flag:
                        data["weibo"].append(new)
            else:
                data["weibo"] += weibo_info
        else:
            data["weibo"] = weibo_info
        return data

    def write_json(self, wrote_count):
        """将爬到的信息写入json文件"""
        data = {}
        path = self.get_filepath("json")
        if os.path.isfile(path):
            with codecs.open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        weibo_info = self.weibo[wrote_count:]
        data = self.update_json_data(data, weibo_info)
        with codecs.open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"{self.got_count}条微博写入json文件完毕,保存:{path}")

    def weibo_to_sqlite(self, wrote_count):
        con = self.get_sqlite_connection()
        weibo_list = []
        retweet_list = []
        if len(self.write_mode) > 1:
            info_list = copy.deepcopy(self.weibo[wrote_count:])
        else:
            info_list = self.weibo[wrote_count:]
        for w in info_list:
            if "retweet" in w:
                w["retweet"]["retweet_id"] = ""
                retweet_list.append(w["retweet"])
                w["retweet_id"] = w["retweet"]["id"]
                del w["retweet"]
            else:
                w["retweet_id"] = ""
            weibo_list.append(w)

        comment_max_count = self.comment_max_download_count
        repost_max_count = self.comment_max_download_count
        download_comment = self.download_comment and comment_max_count > 0
        download_repost = self.download_repost and repost_max_count > 0

        count = 0
        for weibo in weibo_list:
            self.sqlite_insert_weibo(con, weibo)
            if (download_comment) and (weibo["comments_count"] > 0):
                self.get_weibo_comments(
                    weibo, comment_max_count, self.sqlite_insert_comments
                )
                count += 1
                # 为防止被ban抓取一定数量的评论后随机睡3到6秒
                if count % 20:
                    sleep(random.randint(3, 6))
            if (download_repost) and (weibo["reposts_count"] > 0):
                self.get_weibo_reposts(
                    weibo, repost_max_count, self.sqlite_insert_reposts
                )
                count += 1
                # 为防止被ban抓取一定数量的转发后随机睡3到6秒
                if count % 20:
                    sleep(random.randint(3, 6))

        for weibo in retweet_list:
            self.sqlite_insert_weibo(con, weibo)

        con.close()

    def sqlite_insert_comments(self, weibo, comments):
        if not comments or len(comments) == 0:
            return
        con = self.get_sqlite_connection()
        for comment in comments:
            data = self.parse_sqlite_comment(comment, weibo)
            self.sqlite_insert(con, data, "comments")

        con.close()

    def sqlite_insert_reposts(self, weibo, reposts):
        if not reposts or len(reposts) == 0:
            return
        con = self.get_sqlite_connection()
        for repost in reposts:
            data = self.parse_sqlite_repost(repost, weibo)
            self.sqlite_insert(con, data, "reposts")

        con.close()

    def parse_sqlite_comment(self, comment, weibo):
        if not comment:
            return
        sqlite_comment = OrderedDict()
        sqlite_comment["id"] = comment["id"]

        self._try_get_value("bid", "bid", sqlite_comment, comment)
        self._try_get_value("root_id", "rootid", sqlite_comment, comment)
        self._try_get_value("created_at", "created_at", sqlite_comment, comment)
        sqlite_comment["weibo_id"] = weibo["id"]

        sqlite_comment["user_id"] = comment["user"]["id"]
        sqlite_comment["user_screen_name"] = comment["user"]["screen_name"]
        self._try_get_value(
            "user_avatar_url", "avatar_hd", sqlite_comment, comment["user"]
        )
        if self.remove_html_tag:
            sqlite_comment["text"] = re.sub('<[^<]+?>', '', comment["text"]).replace('\n', '').strip()
        else:
            sqlite_comment["text"] = comment["text"]

        sqlite_comment["pic_url"] = ""
        if comment.get("pic"):
            sqlite_comment["pic_url"] = comment["pic"]["large"]["url"]
        self._try_get_value("like_count", "like_count", sqlite_comment, comment)
        return sqlite_comment

    def parse_sqlite_repost(self, repost, weibo):
        if not repost:
            return
        sqlite_repost = OrderedDict()
        sqlite_repost["id"] = repost["id"]

        self._try_get_value("bid", "bid", sqlite_repost, repost)
        self._try_get_value("created_at", "created_at", sqlite_repost, repost)
        sqlite_repost["weibo_id"] = weibo["id"]

        sqlite_repost["user_id"] = repost["user"]["id"]
        sqlite_repost["user_screen_name"] = repost["user"]["screen_name"]
        self._try_get_value(
            "user_avatar_url", "profile_image_url", sqlite_repost, repost["user"]
        )
        text = repost.get("raw_text")
        if text:
            text = text.split("//", 1)[0]
        if text is None or text == "" or text == "Repost":
            text = "转发微博"
        sqlite_repost["text"] = text
        self._try_get_value("like_count", "attitudes_count", sqlite_repost, repost)
        return sqlite_repost

    def _try_get_value(self, source_name, target_name, dict, json):
        dict[source_name] = ""
        value = json.get(target_name)
        if value:
            dict[source_name] = value

    def sqlite_insert_weibo(self, con: sqlite3.Connection, weibo: dict):
        sqlite_weibo = self.parse_sqlite_weibo(weibo)
        self.sqlite_insert(con, sqlite_weibo, "weibo")

    def parse_sqlite_weibo(self, weibo):
        if not weibo:
            return
        sqlite_weibo = OrderedDict()
        sqlite_weibo["user_id"] = weibo["user_id"]
        sqlite_weibo["id"] = weibo["id"]
        sqlite_weibo["bid"] = weibo["bid"]
        sqlite_weibo["screen_name"] = weibo["screen_name"]
        sqlite_weibo["text"] = weibo["text"]
        sqlite_weibo["article_url"] = weibo["article_url"]
        sqlite_weibo["topics"] = weibo["topics"]
        sqlite_weibo["pics"] = weibo["pics"]
        sqlite_weibo["video_url"] = weibo["video_url"]
        sqlite_weibo["location"] = weibo["location"]
        sqlite_weibo["created_at"] = weibo["full_created_at"]
        sqlite_weibo["source"] = weibo["source"]
        sqlite_weibo["attitudes_count"] = weibo["attitudes_count"]
        sqlite_weibo["comments_count"] = weibo["comments_count"]
        sqlite_weibo["reposts_count"] = weibo["reposts_count"]
        sqlite_weibo["retweet_id"] = weibo["retweet_id"]
        sqlite_weibo["at_users"] = weibo["at_users"]
        return sqlite_weibo

    def user_to_sqlite(self):
        con = self.get_sqlite_connection()
        self.sqlite_insert_user(con, self.user)
        con.close()

    def sqlite_insert_user(self, con: sqlite3.Connection, user: dict):
        sqlite_user = self.parse_sqlite_user(user)
        self.sqlite_insert(con, sqlite_user, "user")

    def parse_sqlite_user(self, user):
        if not user:
            return
        sqlite_user = OrderedDict()
        sqlite_user["id"] = user["id"]
        sqlite_user["nick_name"] = user["screen_name"]
        sqlite_user["gender"] = user["gender"]
        sqlite_user["follower_count"] = user["followers_count"]
        sqlite_user["follow_count"] = user["follow_count"]
        sqlite_user["birthday"] = user["birthday"]
        sqlite_user["location"] = user["location"]
        sqlite_user["edu"] = user["education"]
        sqlite_user["company"] = user["company"]
        sqlite_user["reg_date"] = user["registration_time"]
        sqlite_user["main_page_url"] = user["profile_url"]
        sqlite_user["avatar_url"] = user["avatar_hd"]
        sqlite_user["bio"] = user["description"]
        return sqlite_user

    def sqlite_insert(self, con: sqlite3.Connection, data: dict, table: str):
        if not data:
            return
        cur = con.cursor()
        keys = ",".join(data.keys())
        values = ",".join(["?"] * len(data))
        sql = """INSERT OR REPLACE INTO {table}({keys}) VALUES({values})
                """.format(
            table=table, keys=keys, values=values
        )
        cur.execute(sql, list(data.values()))
        con.commit()

    def get_sqlite_connection(self):
        path = self.get_sqlte_path()
        create = False
        if not os.path.exists(path):
            create = True

        con = sqlite3.connect(path, check_same_thread=False, isolation_level=None, timeout=10)

        if create:
            self.create_sqlite_table(connection=con)

        return con

    def create_sqlite_table(self, connection: sqlite3.Connection):
        sql = self.get_sqlite_create_sql()
        cur = connection.cursor()
        cur.executescript(sql)
        connection.commit()

    def get_sqlte_path(self):
        return os.path.join(DATA_DIR, "weibodata.db")

    def get_sqlite_create_sql(self):
        create_sql = """
                CREATE TABLE IF NOT EXISTS user (
                    id varchar(64) NOT NULL
                    ,nick_name varchar(64) NOT NULL
                    ,gender varchar(6)
                    ,follower_count integer
                    ,follow_count integer
                    ,birthday varchar(10)
                    ,location varchar(32)
                    ,edu varchar(32)
                    ,company varchar(32)
                    ,reg_date DATETIME
                    ,main_page_url text
                    ,avatar_url text
                    ,bio text
                    ,PRIMARY KEY (id)
                );

                CREATE TABLE IF NOT EXISTS weibo (
                    id varchar(20) NOT NULL
                    ,bid varchar(12) NOT NULL
                    ,user_id varchar(20)
                    ,screen_name varchar(30)
                    ,text varchar(2000)
                    ,article_url varchar(100)
                    ,topics varchar(200)
                    ,at_users varchar(1000)
                    ,pics varchar(3000)
                    ,video_url varchar(1000)
                    ,location varchar(100)
                    ,created_at DATETIME
                    ,source varchar(30)
                    ,attitudes_count INT
                    ,comments_count INT
                    ,reposts_count INT
                    ,retweet_id varchar(20)
                    ,PRIMARY KEY (id)
                );

                CREATE TABLE IF NOT EXISTS bins (
                    id integer PRIMARY KEY AUTOINCREMENT
                    ,ext varchar(10) NOT NULL /*file extension*/
                    ,data blob NOT NULL
                    ,weibo_id varchar(20)
                    ,comment_id varchar(20)
                    ,path text
                    ,url text
                );

                CREATE TABLE IF NOT EXISTS comments (
                    id varchar(20) NOT NULL
                    ,bid varchar(20) NOT NULL
                    ,weibo_id varchar(32) NOT NULL
                    ,root_id varchar(20) 
                    ,user_id varchar(20) NOT NULL
                    ,created_at varchar(20)
                    ,user_screen_name varchar(64) NOT NULL
                    ,user_avatar_url text
                    ,text varchar(1000)
                    ,pic_url text
                    ,like_count integer
                    ,PRIMARY KEY (id)
                );

                CREATE TABLE IF NOT EXISTS reposts (
                    id varchar(20) NOT NULL
                    ,bid varchar(20) NOT NULL
                    ,weibo_id varchar(32) NOT NULL
                    ,user_id varchar(20) NOT NULL
                    ,created_at varchar(20)
                    ,user_screen_name varchar(64) NOT NULL
                    ,user_avatar_url text
                    ,text varchar(1000)
                    ,like_count integer
                    ,PRIMARY KEY (id)
                );
                """
        return create_sql

    def update_user_config_file(self, user_config_file_path):
        """更新用户配置文件"""
        with open(user_config_file_path, "rb") as f:
            try:
                lines = f.read().splitlines()
                lines = [line.decode("utf-8-sig") for line in lines]
            except UnicodeDecodeError:
                logger.error("%s文件应为utf-8编码，请先将文件编码转为utf-8再运行程序", user_config_file_path)
                sys.exit()
            for i, line in enumerate(lines):
                info = line.split(" ")
                if len(info) > 0 and info[0].isdigit():
                    if self.user_config["user_id"] == info[0]:
                        if len(info) == 1:
                            info.append(self.user["screen_name"])
                            info.append(self.start_date)
                        if len(info) == 2:
                            info.append(self.start_date)
                        if len(info) > 2:
                            info[2] = self.start_date
                        lines[i] = " ".join(info)
                        break
        with codecs.open(user_config_file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    def write_data(self, wrote_count):
        """将爬到的信息写入文件或数据库"""
        if self.got_count > wrote_count:
            if "csv" in self.write_mode:
                self.write_csv(wrote_count)
            if "json" in self.write_mode:
                self.write_json(wrote_count)
            if "sqlite" in self.write_mode:
                self.weibo_to_sqlite(wrote_count)
            if self.original_pic_download:
                self.download_files("img", "original", wrote_count)
            if self.original_video_download:
                self.download_files("video", "original", wrote_count)
            if not self.only_crawl_original:
                if self.retweet_pic_download:
                    self.download_files("img", "retweet", wrote_count)
                if self.retweet_video_download:
                    self.download_files("video", "retweet", wrote_count)

    def get_pages(self, max_blogs):
        """获取全部微博"""
        try:
            # 用户id不可用
            if self.get_user_info() != 0:
                return
            logger.info("准备搜集 {} 的微博".format(self.user["screen_name"]))
            if MODE == "append" and (
                    "first_crawler" not in self.__dict__ or self.first_crawler is False
            ):
                # 本次运行的某用户首次抓取，用于标记最新的微博id
                self.first_crawler = True
                CHECK_COOKIE["GUESS_PIN"] = True
            since_date = datetime.strptime(self.user_config["since_date"], DTFORMAT)
            today = datetime.today()
            if since_date <= today:  # since_date 若为未来则无需执行
                all_page_count = self.get_page_count()
                page_count = min(all_page_count, self.max_page)
                logger.info("共{}页，获取top{}页微博".format(all_page_count, page_count))
                wrote_count = 0
                page1 = 0
                random_pages = random.randint(1, 5)
                self.start_date = datetime.now().strftime(DTFORMAT)
                pages = range(self.start_page, page_count + 1)
                for page in tqdm(pages, desc="Progress"):
                    is_end = self.get_one_page(page)
                    if is_end:
                        break

                    if page % 20 == 0:  # 每爬20页写入一次文件
                        self.write_data(wrote_count)
                        wrote_count = self.got_count

                    # 通过加入随机等待避免被限制。爬虫速度过快容易被系统限制(一段时间后限
                    # 制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。默
                    # 认是每爬取1到5页随机等待6到10秒，如果仍然被限，可适当增加sleep时间
                    if (page - page1) % random_pages == 0 and page < page_count:
                        sleep(random.randint(6, 10))
                        page1 = page
                        random_pages = random.randint(1, 5)

                    if self.got_count >= max_blogs:  # 添加这个条件判断
                        logger.info(f"已获取{self.got_count}条微博，达到设定的最大微博数{max_blogs}，停止获取")
                        break
                self.write_data(wrote_count)  # 将剩余不足20页的微博写入文件
            logger.info(f"微博爬取完成，共爬取{self.got_count}条微博")
        except Exception as e:
            logger.exception(e)

    def get_user_config_list(self, file_path):
        """获取文件中的微博id信息"""
        with open(file_path, "rb") as f:
            try:
                lines = f.read().splitlines()
                lines = [line.decode("utf-8-sig") for line in lines]
            except UnicodeDecodeError:
                logger.error("%s文件应为utf-8编码，请先将文件编码转为utf-8再运行程序", file_path)
                sys.exit()
            user_config_list = []
            # 分行解析配置，添加到user_config_list
            for line in lines:
                info = line.strip().split(" ")  # 去除字符串首尾空白字符
                if len(info) > 0 and info[0].isdigit():
                    user_config = {}
                    user_config["user_id"] = info[0]
                    # 根据配置文件行的字段数确定 since_date 的值
                    if len(info) == 3:
                        if self.is_datetime(info[2]):
                            user_config["since_date"] = info[2]
                        elif self.is_date(info[2]):
                            user_config["since_date"] = "{}T00:00:00".format(info[2])
                        elif info[2].isdigit():
                            since_date = date.today() - timedelta(int(info[2]))
                            user_config["since_date"] = since_date.strftime(DTFORMAT)
                        else:
                            logger.error("since_date 格式不正确，请确认配置是否正确")
                            sys.exit()
                    else:
                        user_config["since_date"] = self.since_date
                    # 若超过3个字段，则第四个字段为 query_list
                    if len(info) > 3:
                        user_config["query_list"] = info[3].split(",")
                    else:
                        user_config["query_list"] = self.query_list
                    if user_config not in user_config_list:
                        user_config_list.append(user_config)
        return user_config_list

    def initialize_info(self, user_config):
        """初始化爬虫信息"""
        self.weibo = []
        self.user = {}
        self.user_config = user_config
        self.got_count = 0
        self.weibo_id_list = []

    def start(self, max_blogs=12):
        """运行爬虫"""
        try:
            for user_config in self.user_config_list:
                if len(user_config["query_list"]):
                    for query in user_config["query_list"]:
                        self.query = query
                        self.initialize_info(user_config)
                        self.get_pages(max_blogs)
                else:
                    self.initialize_info(user_config)
                    self.get_pages(max_blogs)
                if self.user_config_file_path and self.user:
                    self.update_user_config_file(self.user_config_file_path)
                if self.user:
                    self.screen_names.append(self.user["screen_name"])  # 添加这一行
            return self.screen_names  # 添加这一行
        except Exception as e:
            logger.exception(e)


def parse_response_users(response):
    result = []
    try:
        html = etree.HTML(response.text)
        users = html.xpath('//div[starts-with(@class,"card card-user-b")]')
        if not users:
            return result

        for user in users:
            userid = user.xpath('.//div[@class="avator"]/a/@href')
            if userid:
                userid = userid[0].split('/')[-1]
            username = user.xpath('.//a[@class="name"]/text()')
            if username:
                username = username[0]
            result.append({'userid': userid, 'username': username})

        return result
    except Exception as e:
        logger.error(f"Error parsing response: {e}")
        return result


similarity_model = None
user_dict = dict()


def find_users_from_local_csv(name):
    logger.info(f"Searching users from local csv: {name}")
    output_users = []
    try:
        from similarities import SameCharsSimilarity
    except:
        logger.error("Please install the similarities library: pip install similarities")
        return output_users

    # 使用相似度算法找到最相似的用户
    global similarity_model
    if similarity_model is None:
        csv_file = os.path.join(DATA_DIR, 'users.csv')

        usernames = []
        if not user_dict:
            # read csv file, 并提取出字段：用户id、昵称
            df = pd.read_csv(csv_file, encoding='utf-8')
            for _, row in df.iterrows():
                user_id = row.get('用户id')
                username = row.get('昵称')
                if user_id and username:
                    user_dict[username] = user_id
                    usernames.append(username)
        if not usernames:
            logger.error("No usernames found in local csv")
            return output_users
        usernames = list(set(usernames))
        logger.info(f"Read {len(usernames)} usernames from local csv, "
                    f"top3: {usernames[:3]}, user_dict len: {len(user_dict)}")
        similarity_model = SameCharsSimilarity(corpus=usernames)
    sim_res = similarity_model.search(name, topn=5)
    logger.debug(f"Similarity search result: {sim_res}")
    sim_usernames = [i.get('corpus_doc', '') for i in sim_res[0]]

    for username in sim_usernames:
        # 'userid': userid, 'username': username
        user_id = user_dict.get(username)
        if user_id and username:
            output_users.append({'userid': user_id, 'username': username})
    logger.info(f"Found users from local csv: {output_users}, query: {name}")
    return output_users


def find_users_by_name(name):
    url = 'https://s.weibo.com/user'
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?10',
        'Sec-Fetch-Dest': 'document',
        'Referer': 'https://s.weibo.com/weibo?q=%E6%B5%8B%E8%AF%95',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-CN;q=0.8,en;q=0.7,es-MX;q=0.6,es;q=0.5',
        'cookie': weibo_cookie
    }
    params = {
        'q': name,
        'Refer': 'weibo_user',
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            users = parse_response_users(response)
            if not users:
                users = find_users_from_local_csv(name)
            return users
        else:
            logger.error(f"Failed to get response, status code: {response.status_code}")
            return find_users_from_local_csv(name)
    except Exception as e:
        logger.error(f"Error fetching UID by name: {e}")
        return find_users_from_local_csv(name)


def get_user_url_by_id(user_id):
    return f"https://weibo.com/u/{user_id}"


def df_add_user_link(file_path, user_name_column, user_link_column='user_link', finish_column='finish'):
    df = pd.read_csv(file_path)
    if finish_column not in df.columns:
        df[finish_column] = False
        df[user_link_column] = ''
        df.to_csv(file_path, index=False, encoding='utf-8-sig')

    consist = 0
    consist_limit = 3
    for idx, row in df.iterrows():
        print(f'{idx + 1}/{df.shape[0]}')
        if row[finish_column]:
            continue

        uid = find_users_by_name(row[user_name_column])
        if uid == -1:
            consist += 1
            if consist >= consist_limit:
                print('请检查是否需要换 cookie')
                df.to_csv(file_path, index=False, encoding='utf-8-sig')
                break
        else:
            consist = 0
            user_link = f"https://weibo.com/u/{uid[0]}" if uid else ''
            print(user_link)
            df.at[idx, user_link_column] = user_link
            df.at[idx, finish_column] = True

        if idx % 10 == 0:
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
    df.to_csv(file_path, index=False, encoding='utf-8-sig')


def convert_to_days_ago(date_str, how_many_days):
    """将日期字符串转换为多少天前的日期字符串"""
    date_str = datetime.strptime(date_str, DTFORMAT)
    date_str = date_str + timedelta(days=-how_many_days)
    return date_str.strftime(DTFORMAT)


def insert_or_update_user(logger, headers, result_data, file_path):
    """插入或更新用户csv。不存在则插入，最新抓取微博id不填，存在则先不动，返回已抓取最新微博id和日期"""
    first_write = True if not os.path.isfile(file_path) else False
    if os.path.isfile(file_path):
        # 文件已存在，直接查看有没有，有就直接return了
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.split(',')[0] == result_data[0][0]:
                    return line.split(',')[len(line.split(',')) - 1].replace('\n', '')

    # 没有或者新建
    result_data[0].append('')
    with open(file_path, 'a', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        if first_write:
            writer.writerows([headers])
        writer.writerows(result_data)
    logger.info('{} 信息写入csv文件完毕，保存路径: {}'.format(result_data[0][1], file_path))
    return ''


def update_last_weibo_id(userid, new_last_weibo_msg, file_path):
    """更新用户csv中的最新微博id"""
    lines = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.split(',')[0] == str(userid):
                line = line.replace(line.split(
                    ',')[len(line.split(',')) - 1], new_last_weibo_msg + '\n')
            lines.append(line)
        f.close()
    with open(file_path, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line)


def crawl_weibo_content_by_userids(user_id_list, max_blogs=20):
    try:
        wb = WeiboCrawler(user_id_list)
        screen_names = wb.start(max_blogs)  # 爬取微博信息
        return screen_names
    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    users = find_users_by_name('张伟')
    # return [{'userid': '1746664450', 'username': '大张伟'}, {'userid': '3313256852', 'username': '张伟丽MMA'}, ...]
    print(users)
    users = find_users_by_name('美国')
    print(users)
    # screen_names = crawl_weibo_content_by_userids(['1685786430', '5977585782', '2717789122'])
    # print(f"screen_names: {screen_names}")
