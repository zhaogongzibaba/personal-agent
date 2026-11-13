# Personal Agent 决策治理原型

这是一个只使用 Python 标准库的本地命令行原型。行情、新闻、B 任务输入和“发送”都在本机模拟，不调用真实行情、新闻或邮件服务。

## 一条复跑命令

```powershell
python main.py demo
```

命令会依次展示日报缺项被拦截和修正、批准绑定收件人与日报版本、A 等待期间 B 使用执行槽、收件人变更使旧批准失效、发送记录落盘后模拟退出并在重启后去重接续，以及 RSI 调度策略改进前后的同场景对照。每轮输出保存在 `.personal_agent/demo/<运行编号>/`，终端会打印完整目录。

主要产物：

- `report-<工作流编号>.md`：含价格、单位、日期和来源的日报。每个工作流独立保存，失败重试不会误展示上一轮日报。
- `b-check-result.md`：缺少价格单位的行情记录。
- `send-log.jsonl`：本地模拟发送记录；不会向外发送。
- `events.jsonl`：每次放行、拦截、等待和执行槽释放的原因。
- `rsi-comparison.md` 和 `rsi-comparison.json`：策略改进前后的执行轨迹与指标。
- `state.json`：可恢复的任务状态、批准快照和回执。

## 手动批准流程

```powershell
python main.py run
python main.py status
python main.py set-recipient 李四
python main.py approve --recipient 李四
python main.py send
```

日报和 B 文件检查会自动运行。`run` 在 A 等待批准时返回；发送命令只会在当前收件人与批准对象、日报版本和批准时快照全部一致时写入本地记录。若进程在记录落盘后中断，运行 `python main.py recover` 会复用原记录恢复回执。

## 本地数据配置

当前没有需要填写的密钥或外部服务配置，程序默认使用项目内的模拟数据。需要换成自己的本地样例时，编辑以下文件：

- `data/market_snapshot.json`：设置 `as_of` 日期，以及 `quotes` 行情数组和 `news` 新闻数组。每条行情必填 `symbol`、`name`、非负数值 `price`、`unit`、`date`、`source`；每条新闻必填 `title`、`source`、`impact`。日期格式为 `YYYY-MM-DD`。
- `data/market_check.csv`：保留列 `record_id,symbol,name,price,unit,date,source`。价格非空且有效、但 `unit` 为空时会报告缺少价格单位；空价格和无效日期会作为各自的数据问题报告。
- 收件人：默认是“张三”，可用 `python main.py set-recipient 李四` 修改。批准命令必须再次明确传入同一个当前收件人。

输入文件不存在、JSON 无法解析、行情字段不完整、CSV 必需列缺失或行列数不匹配时，对应任务会记录为 `FAILED`，并保留原因；B 检查失败时，`b-check-result.md` 会明确写明未完成，不会显示成通过。修复数据后再次运行 `python main.py run` 会创建新工作流重试；若 B 失败，即使 A 已等待批准，也会重建工作流，避免拿旧批准继续处理新的数据状态。

同一工作目录的状态变更使用本机文件锁串行化，防止多个 CLI 进程并发批准或重复写发送记录。这适用于单机本地目录，不适用于网络共享盘或多台机器共同写入。

## 测试

```powershell
python -m unittest discover -s tests -v
```
