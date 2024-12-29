# -*- coding: utf-8 -*-
"""
@author:XuMing(xuming624@qq.com)
@description:
"""

from agentica import Message, DeepSeekChat, OpenAIChat, MoonshotChat

# from dotenv import load_dotenv
# load_dotenv() # default .env path: ~/.agentica/.env
llm = DeepSeekChat()


def llm_response(messages):
    if isinstance(messages, str):
        messages = [Message(role="user", content=messages)]
    elif isinstance(messages, list):
        llm_messages = []
        # If messages are provided, simply use them
        if messages is not None and len(messages) > 0:
            for _m in messages:
                if isinstance(_m, Message):
                    llm_messages.append(_m)
                elif isinstance(_m, dict):
                    llm_messages.append(Message.model_validate(_m))
        messages = llm_messages
    return llm.response_stream(messages)


if __name__ == '__main__':
    prompt = "一句话介绍北京?"
    print('llm:', llm)
    r = llm_response(prompt)
    print(r)
    for i in r:
        print(i)
