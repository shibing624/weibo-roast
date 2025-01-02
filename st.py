# -*- coding: utf-8 -*-
"""
@author:XuMing(xuming624@qq.com)
@description:
"""

import os
import streamlit as st
import json
from prompts import get_tucao_dangerous_prompt
from weibo_crawler import crawl_weibo_content_by_userids, find_users_by_name, DATA_DIR
from ask_llm import llm_response
from loguru import logger


def crawl_weibo(user_id: str, user_name: str, max_blogs: int = 15):
    user_id_list = [user_id]
    logger.info(f"🔍 搜索博主：{user_name}，链接：{user_id}")
    user_file = os.path.join(DATA_DIR, f'{user_name}/{user_id}.json')
    data = None
    if os.path.exists(user_file):
        logger.debug(f"从缓存中读取博主：{user_name}")
        with open(user_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        try:
            screen_names = crawl_weibo_content_by_userids(user_id_list, max_blogs)[0]
            logger.debug(f"user_id_list：{user_id_list}，博主昵称：{screen_names}")
            if os.path.exists(user_file):
                with open(user_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
        except Exception as e:
            logger.error(f"🔍 搜索博主：{user_name}，链接：{user_id} 失败, {e}")
            st.error(f"😣 搜索博主失败：{e}")
            st.stop()
    if not data:
        st.error("😣 找不到你说的博主 请换一个博主试试")
        st.stop()
    profile = f"{data['user']['screen_name']}, {data['user']['verified_reason']}\n{data['user']['description']}"
    blogs = '\n'.join([weibo['text'].replace("\n", "\\n") for weibo in data['weibo'][:max_blogs]])  # 转义换行符
    return profile, blogs


def generate_tucao(profile: str, blogs: str):
    try:
        tucao_dangerous_prompt = get_tucao_dangerous_prompt(profile=profile, blogs=blogs)
        tucao_dangerous = llm_response(tucao_dangerous_prompt)
        logger.debug(f"初步吐槽：\n{tucao_dangerous}")

        # 流式返回吐槽内容
        for chunk in tucao_dangerous:
            if chunk and hasattr(chunk, 'content'):
                chunk_str = chunk.content
                yield chunk_str
    except Exception as e:
        logger.error(f"生成吐槽失败: {e}")
        st.error("😣 服务器繁忙，请稍后再试")
        st.stop()


def main():
    st.set_page_config(layout="centered", page_title="微博吐槽", page_icon="🤭")

    st.markdown(
        """
        <style>
        .stApp {
            margin: 0 auto;
            font-family: 'Arial, sans-serif';
        }
        .output-card {
            border-radius: 10px;
            border: 1px solid #e0e0e0;
            padding: 20px;
            margin-top: 20px;
            background-color: #f9f9f9;
            font-size: 18px;
        }
        .emoji {
            font-size: 24px;
            margin-right: 10px;
        }
        .header {
            text-align: left;
            margin-bottom: 20px;
        }
        .btn-link {
            display: inline-block;
            padding: 0.5em 1em;
            color: white;
            background-color: #87CEEB;
            border-radius: 6px;
            text-decoration: none;
            font-weight: bold;
            transition: background-color 0.3s ease;
        }
        .btn-link:hover {
            background-color: #00BFFF;
        }
        </style>
        """, unsafe_allow_html=True)

    st.title("🤭 微博吐槽")
    st.markdown(
        """
        <div class="header">
            <a href="https://github.com/shibing624/weibo-roast" target="_blank" class="btn-link">
                ⭐ Github点亮星星[https://github.com/shibing624/weibo-roast]
            </a>
        </div>
        """, unsafe_allow_html=True)

    st.info("👉 本项目使用 LLM Agent 生成微博吐槽，仅供娱乐，不代表任何立场")

    # 初始化会话状态
    if "users" not in st.session_state:
        st.session_state.users = []
    if "selected_user" not in st.session_state:
        st.session_state.selected_user = None

    user_name = st.text_input("📝 输入博主的昵称")
    if user_name:
        find_users = find_users_by_name(user_name)
        if find_users == -1 or find_users == []:
            st.session_state.users = []
            st.error("😣 找不到你说的博主 请换一个博主试试")
            st.stop()
        else:
            st.session_state.users = find_users

    # 显示候选博主列表
    candidates = [user["username"] for user in st.session_state.users[:5]] if st.session_state.users else []
    selected_user_name = st.selectbox("🔍 选择一个博主", options=candidates)

    # 如果选择了候选博主，处理并生成吐槽
    if selected_user_name:
        st.session_state.selected_user = next(
            user for user in st.session_state.users if user["username"] == selected_user_name)

    # 处理选中的博主并生成吐槽
    if st.session_state.selected_user:
        chat_box = st.empty()
        user_id = st.session_state.selected_user["userid"]
        user_name = st.session_state.selected_user["username"]
        with st.spinner(f"📱 正在搜集 {user_name} 微博内容..."):
            profile, blogs = crawl_weibo(user_id, user_name)
        logger.info(f"{user_name} 博主简介：{profile}\n\n博主微博：{blogs}")

        tucao_title = f"吐槽--{user_name} 🤣"

        with st.spinner(f"🤣 正在吐槽 {user_name}..."):
            tucao_safe = ""
            for chunk in generate_tucao(profile, blogs):
                tucao_safe += chunk
                chat_box.markdown(f'<div class="output-card"><h3>{tucao_title}</h3>\n\n\n{tucao_safe}</div>',
                                  unsafe_allow_html=True)

        # 完成吐槽后展示气球动画
        st.balloons()

    # 清空聊天框内容
    if user_name or selected_user_name:
        st.session_state.selected_user = None


if __name__ == '__main__':
    main()
