# 短信记账规则（95588 / 工商银行）

本目录用于存放“短信 → 结构化交易”的解析规则与分类映射，方便后续对接 Firefly III/YNAB 等记账软件 API。

## 1) 数据来源（iMessage / chat.db）

你提供的查询命令可以直接查看 95588 的短信：

```sh
sqlite3 ~/Library/Messages/chat.db "
SELECT
  datetime(message.date / 1000000000 + strftime('%s', '2001-01-01'), 'unixepoch', 'localtime') AS date,
  handle.id AS sender,
  message.text
FROM message
JOIN handle ON message.handle_id = handle.ROWID
WHERE handle.id LIKE '%95588%'
ORDER BY message.date DESC;"
```

## 导出为 JSONL（推荐用于规则分析）

把 95588 全量消息导出为 `jsonl`（每行一个 JSON，包含 `text`、解码后的 `attributedBody`、以及合并后的 `content`）：

```sh
cd <repo-root>
python3 -m qbillrecord run --pipeline pipelines/qbillrecord_icbc95588_inc.yml
```

只导出最近 N 条（调试用）：

```sh
python3 -m qbillrecord run --pipeline pipelines/qbillrecord_icbc95588_inc.yml
```

## 导出为 Firefly III 写入格式（JSONL）

基于当前 `rules` 把每条“余额变动短信”转换成 Firefly III `POST /v1/transactions` 的 payload（口径B：按商户/对方自动建账户；分类仅“收入/支出”；“工资薪酬”等细分作为 tags）：

```sh
python3 -m qbillrecord run --pipeline pipelines/qbillrecord_icbc95588_inc.yml
```

### 1.1 为什么有的 `message.text` 为空？

iMessage 里有些消息的可见文本只存放在 `message.attributedBody`（富文本归档字段），此时 `message.text` 会是 `NULL`。

如果你想把这些 `attributedBody` 里的文本也解出来，可用：

```sh
# This legacy script was removed during the CLI rebuild.
```

## 2) 规则文件

- `rules/icbc_95588_rules.json`
  - `ignore_if_text_matches_any`：安全/验证码/登录提醒等非账务短信的过滤关键词
  - `transaction_patterns`：余额变动类短信的正则解析（支出/收入、渠道、商户、金额、余额、卡尾号、发生时间）
  - `category_taxonomy`：分类体系（目前已先固定“收入”大类下的子类）
  - `category_rules`：根据商户/业务类型做分类建议（无法识别则进 `待分类` 并打 `needs_review`）
  - `tags_rules`：按支付渠道打标签（财付通/支付宝/拼多多等）

## 3) 当前基于样本已覆盖的短信形态

余额变动（支出/收入）：

- `尾号xxxx卡M月D日HH:MM支出(消费财付通-商户)xx.xx元，余额xx.xx元`
- `尾号xxxx卡M月D日HH:MM收入(退款财付通-说明)xx元，余额xx.xx元`
- `尾号xxxx卡M月D日HH:MM支出(缴费财付通-说明)0.17元，余额xx.xx元`
- `尾号xxxx卡M月D日HH:MM工商银行支出(信使展期服务费)3元，余额xx.xx元`

非账务短信（应忽略）：

- 动态密码/验证码/快捷支付开通/登录安全提醒/工银信使服务变更等
