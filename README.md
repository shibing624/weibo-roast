# weibo-roast
基于 agentica 构建了一个微博毒舌AI，疯狂 diss 微博博主。

微博吐槽（weibo-roast）是一个微博毒舌AI，集成了微博数据抓取、处理、分析和生成幽默毒舌评论的功能。

1. 通过`weibo-crawler.py`爬取指定博主的近30条微博
2. 基于爬取的微博和评论，调用 LLM 对微博博主刻薄"吐槽"

## Demo

Demo: [http://180.76.159.247:8501/](http://180.76.159.247:8501/)

<img src="https://github.com/shibing624/weibo-roast/blob/main/docs/dazhagnwei.png" width="600" />

## 快速开始

### 1. 安装依赖项
```shell
pip install -r requirements.txt
```

### 2. 配置环境变量

### 2.1 配置 LLM API

find your Deepseek API key in [Deepseek](https://platform.deepseek.com/api_keys) and set it in environment: 
```shell
export DEEPSEEK_API_KEY="sk-..."
````


### 2.2 配置微博cookie
配置自己的cookie（先在网页端登陆微博然后打开https://weibo.cn 然后F12-Network中的Headers->Request Headers-Cookie 

如何获取weibo cookie具体步骤：

1.用Chrome打开<https://passport.weibo.cn/signin/login>；

2.输入微博的用户名、密码，登录，如图所示：
![](https://picture.cognize.me/cognize/github/weibospider/cookie1.png)
登录成功后会跳转到<https://m.weibo.cn>;

3.按F12键打开Chrome开发者工具，在地址栏输入并跳转到<https://weibo.cn>，跳转后会显示如下类似界面:
![](https://picture.cognize.me/cognize/github/weibospider/cookie2.png)
4.依此点击Chrome开发者工具中的Network->Name中的weibo.cn->Headers->Request Headers，"Cookie:"后的值即为我们要找的cookie值，复制即可，如图所示：
![](https://picture.cognize.me/cognize/github/weibospider/cookie3.png)


set WEIBO_COOKIE in environment: 
```shell
export WEIBO_COOKIE="your cookie"
```


### 3. 启动streamlit
```shell
streamlit run st.py
```


## 高级用法

- 修改 `prompts.py` 中的提示以自定义 AI 生成的内容
- 修改 `ask_llm.py` 中的 `llm` , 推荐效果 OpenAILLM(gpt-4o) > DeepseekLLM(deepseek-coder) > MoonshotLLM(moonshot-v1-8k)

## Contact

- Issue(建议)
  ：[![GitHub issues](https://img.shields.io/github/issues/shibing624/agentica.svg)](https://github.com/shibing624/agentica/issues)
- 邮件我：xuming: xuming624@qq.com
- 微信我： 加我*微信号：xuming624, 备注：姓名-公司-NLP* 进NLP交流群。

<img src="https://github.com/shibing624/weibo-roast/blob/main/docs/wechat.jpeg" width="200" />

<img src="https://github.com/shibing624/weibo-roast/blob/main/docs/wechat_group.jpg" width="200" />


## Citation

如果你在研究中使用了`weibo-roast`，请按如下格式引用：

APA:

```
Xu, M. weibo-roast: Weibo Roast AI (Version 0.0.1) [Computer software]. https://github.com/shibing624/weibo-roast
```

BibTeX:

```
@misc{Xu_weibo-roast,
  title={weibo-roast: Weibo Roast AI},
  author={Xu Ming},
  year={2024},
  howpublished={\url{https://github.com/shibing624/weibo-roast}},
}
```

## License

授权协议为 [The Apache License 2.0](/LICENSE)，可免费用做商业用途。请在产品说明中附加`weibo-roast`的链接和授权协议。
## Contribute

项目代码还很粗糙，如果大家对代码有所改进，欢迎提交回本项目，在提交之前，注意以下两点：

- 在`tests`添加相应的单元测试
- 使用`python -m pytest`来运行所有单元测试，确保所有单测都是通过的

之后即可提交PR。

## 致谢

- [weibo-crawler](https://github.com/dataabc/weibo-crawler) 
- [WeiboSuperSpider](https://github.com/Python3Spiders/WeiboSuperSpider) 
- [agentica](https://github.com/shibing624/agantica)
- [WeiboRoast](https://github.com/Huanshere/WeiboRoast/tree/main)