# -*- coding: utf-8 -*-
"""
@author:XuMing(xuming624@qq.com)
@description:
"""

from agentica import Message
from agentica import Assistant, DeepseekLLM
from agentica.tools.file import FileTool
from loguru import logger

assistant = Assistant(
    llm=DeepseekLLM(),
    tools=[FileTool()],
    add_datetime_to_instructions=True,
    show_tool_calls=True,
    read_chat_history=True,
    debug_mode=False,
)
logger.debug(f"assistant loaded, assistant={assistant}")


def llm_response(messages, response_json=False):
    if response_json:
        llm = DeepseekLLM(response_format={"type": "json_object"})
    else:
        llm = DeepseekLLM()
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


def assistant_response(prompt):
    r = assistant.run(prompt)
    print(r, "".join(r))
    return r


if __name__ == '__main__':
    prompt = "一句话介绍alpaca?"
    r = llm_response(prompt)
    print(r)
    for i in r:
        print(i)

    prompt = """
按照以下格式，输入待评分的文本，返回敏感度评分结果。

- 响应格式
用JSON格式返回评分结果。

- 示例
```json
{{"sensitive": 1}}
```
### 问题
alpaca是哺乳动物吗?"
"""
    print(llm_response(prompt, response_json=True))
    prompt = "一句话介绍北京"
    r = assistant_response(prompt)
    print(r, "".join(r))
