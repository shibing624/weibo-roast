# weibo-roast
基于 agentica 构建了一个微博毒舌AI，疯狂 diss 微博博主。

微博吐槽（weibo-roast）是一个微博毒舌AI，集成了微博数据抓取、处理、分析和生成幽默毒舌评论的功能，通过weibo-crawler爬取指定用户的近20条原创微博，然后调用 GPT 模型生成对微博博主的"吐槽"。

## 快速开始

![demo](https://github.com/user-attachments/assets/bbcf26bd-2072-429c-9b50-876adfa6d9e8)


```shell
pip install -r requirements.txt
cp .env.example .env
python app.py
```

## 高级用法

- 修改 `prompts.py` 中的提示以自定义 AI 生成的内容
- 修改 `app.py` 中的 `model` , 推荐效果 sonnet > Qwen1.5 72B > deepseek-coder

## Contact

- Issue(建议)
  ：[![GitHub issues](https://img.shields.io/github/issues/shibing624/agentica.svg)](https://github.com/shibing624/agentica/issues)
- 邮件我：xuming: xuming624@qq.com
- 微信我： 加我*微信号：xuming624, 备注：姓名-公司-NLP* 进NLP交流群。

<img src="https://github.com/shibing624/weibo-roast/blob/main/docs/wechat.jpeg" width="200" />

<img src="https://github.com/shibing624/weibo-roast/blob/main/docs/wechat_group.jpg" width="200" />


## Citation

如果你在研究中使用了`agentica`，请按如下格式引用：

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