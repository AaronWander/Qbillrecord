# jizhang：95588 → Firefly 流水线架构梳理

本仓库当前形态是一个 **`python -m jizhang`** 驱动的 CLI 项目，采用“pipeline（可替换步骤）”的方式组织：source → transform → sink。
它具备清晰的端到端链路，并通过 YAML pipeline 配置实现输入源/解析规则/分类规则/输出端的可替换与扩展。

## 1. 一句话定位（Why / What）

从输入源导出原始消息（当前内置：macOS iMessage sender=95588）→ 解析+规则分类 → 可选 AI 补全“待分类” → 导出 Firefly III 交易 JSONL → 通过 API 推送入账，并支持增量水位与可回放产物归档。

## 2. 边界与非目标（Scope）

**本项目负责**
- 导出：从 `chat.db` 抽取指定 sender 的短信为 JSONL
- 解析：从短信内容解析出交易（方向/金额/卡尾号/商户/余额等）
- 分类：基于 `rules/*.json` 做规则分类，并可用 DeepSeek 对“待分类”补全
- 导出：生成符合 Firefly III `POST /api/v1/transactions` 的 JSONL payload
- 推送：将 JSONL 推送到 Firefly III，并做幂等/去重与资产账户 bootstrap
- 归档：每次运行写入 `exports/runs/<timestamp>/` 便于审计/重放

**明确不做（当前）**
- 不做 Firefly 的 UI/服务端部署与权限管理（依赖外部 Firefly III）
- 不做“多银行/多短信模板”的通用化（当前聚焦 ICBC 95588）
- 不做严谨财务对账系统（仅提供月度对账/审计脚本辅助）

## 3. 总体架构（静态）

### 3.1 分层视图

- **控制面 / 编排层**
  - `jizhang/cli.py`：统一 CLI（`python -m jizhang ...`）
  - `jizhang/pipeline/runner.py`：pipeline 编排与 run artifacts

- **执行面 / 处理层（可替换 steps）**
  - state：`jizhang/steps/state_rowid.py`
  - source：`jizhang/steps/source_imessage.py`
  - transform：`jizhang/steps/transform_icbc95588.py`（内部调用 `jizhang/transform/icbc95588_pipeline.py`）
  - sink：`jizhang/steps/sink_firefly.py`

- **扩展面 / 配置&规则**
  - Pipeline 配置：`pipelines/*.yml`
  - 规则与 taxonomy：`rules/*.json`
  - 环境变量：`.env` / `.env.example`
  - 产物目录：`exports/`（默认不进 git）

### 3.2 组件职责表（“脚本即组件”）

| 组件/脚本 | 职责 | 主要输入 | 主要输出 |
|---|---|---|---|
| `scripts/export_imessage_sender.py` | 从 Messages `chat.db` 导出 sender 短信到 JSONL；统一 `content` | `chat.db` | `exports/...jsonl`（raw） |
| `scripts/validate_imessage_export.py` | 导出 JSONL 的保守校验（空 content/纯附件等） | raw JSONL | alerts 列表/JSONL |
| `scripts/classify_95588_jsonl_to_md.py` | 规则解析短信为交易；规则分类；渲染 Markdown 分类报告 | raw JSONL + rules | `reports/...md` |
| `scripts/ai_classify_from_classified.py` | 从分类 md 里抽“待分类”批量问 DeepSeek；写 request/response JSONL | classified md + rules | deepseek 请求/响应 JSONL |
| `scripts/pipeline_95588_classify_with_ai.py` | 端到端“分类+AI 应用+导出 Firefly JSONL”；可写审计文件 | raw JSONL + rules | `firefly_*.jsonl` + `ai_audit/` |
| `scripts/export_firefly_transactions_from_jsonl.py` | 将解析交易映射为 Firefly TransactionStore payload JSONL | raw JSONL + rules | Firefly payload JSONL |
| `scripts/push_firefly_jsonl.py` | Firefly API 推送；可 bootstrap 资产账户；去重/状态记录 | firefly JSONL | `push_state.jsonl` |
| `scripts/run_incremental_95588_to_firefly.py` | 增量编排：水位 → 导出 delta → pipeline → push → 更新水位 | state + chat.db + rules | `exports/runs/<ts>/...` + state |
| `scripts/jizhang.py` | 交互式聚合入口（增量/全量/重放） | 用户选择 | 调用上述组件 |

## 4. 核心数据对象（对象模型）

虽然现在脚本之间主要通过 JSONL/MD 传递，但隐含的数据对象已经很明确：

- **RawMsg（导出 JSONL 行）**
  - 关键字段：`rowid`、`date_local`、`sender`、`content`
  - 来源：`export_imessage_sender.py`

- **ParsedTxn（解析后的交易）**
  - 关键字段：`direction_cn`（收入/支出）、`amount`、`card_last4`、`merchant`、`counterparty`、`raw_bracket`、`short_info`
  - 来源：`classify_95588_jsonl_to_md.py:parse_txn`

- **Firefly TransactionStore payload（推送行）**
  - 关键字段：`type`（deposit/withdrawal）、`amount`、`date`、`source_name/destination_name`、`category_name`、`external_id`、`notes`
  - 来源：`export_firefly_transactions_from_jsonl.py`

## 5. 关键链路（动态）

### 5.1 增量链路（推荐日常使用）

入口：`scripts/run_incremental_95588_to_firefly.py`

1) 读取水位：`exports/95588_state.json:last_rowid`
2) 导出 delta：`export_imessage_sender.py --since-rowid <last_rowid>` → `exports/runs/<ts>/raw_delta.jsonl`
3) 校验导出：`validate_imessage_export.py`（发现异常则中止，产出 alerts）
4) 分类+AI+导出：`pipeline_95588_classify_with_ai.py` → `exports/runs/<ts>/firefly_delta.jsonl` + `exports/runs/<ts>/ai_audit/*`
5) 推送：`push_firefly_jsonl.py` → `exports/runs/<ts>/push_state.jsonl`
6) 更新水位：写回 `exports/95588_state.json:last_rowid=<new_max>`

**幂等/去重语义**
- 本地：`push_firefly_jsonl.py` 可用 `--state` 做“已成功 external_id”跳过（可选）
- 服务端：默认开启 Firefly 的 duplicate hash 保护（`error_if_duplicate_hash=true`）
- 稳定外部 ID：`export_firefly_transactions_from_jsonl.py` 用 sha256 生成 `external_id`

### 5.2 全量链路（用于首次导入/重建）

入口：`scripts/jizhang.py` 的“全量”选项（内部调用 export + pipeline + push）。

差异点：
- 全量不会更新 `exports/95588_state.json`（避免误移动增量水位）
- 仍会产出 per-run 归档目录，便于回放

## 6. 运维闭环（配置、产物、排障入口）

### 6.1 必要配置（.env）

repo 根目录 `.env`（示例见 `.env.example`）：
- DeepSeek（可选，`--no-ai` 时可不配）
  - `DEEPSEEK_API_KEY`
  - `DEEPSEEK_BASE_URL`（默认 `https://api.deepseek.com`）
  - `DEEPSEEK_MODEL`（默认 `deepseek-chat`）
  - `DEEPSEEK_API`（可选，默认 `openai-chat-completions`；填 `openai-responses` 时走 `/v1/responses`）
  - 注意：`DEEPSEEK_BASE_URL` 指向的服务必须兼容 OpenAI 的 `POST /v1/chat/completions` 并返回 JSON；否则会在解析响应时报错（例如 `JSONDecodeError`/“Non-JSON response”）。
- Firefly（推送必需）
  - `FIREFLY_BASE_URL`（如 `http://localhost:8080`）
  - `FIREFLY_TOKEN`

### 6.2 产物目录（强建议保留）

- `exports/95588_all.jsonl`：全量导出（或作为输入样本）
- `exports/95588_state.json`：增量水位（ROWID）
- `exports/runs/<timestamp>/`：每次运行归档（raw/firefly/push_state/ai_audit）
- `reports/`：可读报告（markdown）

### 6.3 常见排障入口

- 导出异常：`python3 scripts/validate_imessage_export.py --in <raw.jsonl>`
- “应该能解析却解析不了”：`python3 scripts/audit_force_parse_unparsed.py --in <raw.jsonl>`
- Firefly 推送失败：看 `exports/runs/<ts>/push_state.jsonl` 与 stderr 输出（脚本会打印 status/body 摘要）

## 7. 架构图（Mermaid）

仓库内图文件：`docs/diagrams/pipeline.mmd`（可在支持 Mermaid 的编辑器预览）。

## 8. 怎么把它“做成真正的项目”（演进路线）

建议按“可回滚的小步”做 3 个阶段，每阶段都可独立验收：

### Stage A：文档化 + 统一入口（低风险）
- 新增 `docs/ARCHITECTURE.md` 与流程图（已完成）
- 给一个稳定命令入口（例如 `python3 -m jizhang ...` 或 `jizhang` CLI），脚本仍可复用
- 把所有 I/O 路径与环境变量集中成一个 config 模块（避免每个脚本各读一遍）

### Stage B：模块化（把脚本“收敛为包”）
- 引入 `src/jizhang/`（或 `jizhang/`）包结构：
  - `ingest/`（导出 raw）
  - `parse/`（解析交易）
  - `classify/`（规则分类 + AI 分类）
  - `export/`（Firefly payload）
  - `push/`（Firefly API）
  - `cli/`（Typer/argparse 统一入口）
- 保留 `scripts/` 作为 thin wrapper（兼容老命令），内部调用包函数

### Stage C：测试与可移植性（提高稳定性）
- 为规则解析与 external_id 生成加单测（纯函数、低成本、高收益）
- 把“macOS 专属导出”（PyObjC/Foundation 依赖）隔离：在 Linux/CI 上仍能跑下游（用已导出的 JSONL）
- 增加 `Makefile` 或 `justfile`：固化常用命令与参数，避免口口相传
