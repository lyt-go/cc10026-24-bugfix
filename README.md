# 数据服务开放与订阅管理平台

面向数据API注册、查询路由、订阅审批、用量计量与SLA保障的企业数据服务开放平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| API注册 | `api_registry` | 数据API | API编号、API名称、接口路径 |
| 查询路由 | `query_route` | 路由规则 | 规则编号、关联API、路由条件 |
| 订阅管理 | `subscription` | 订阅申请 | 申请编号、订阅API、申请方 |
| 频率限制 | `rate_limiter` | 限流策略 | 策略编号、目标API、调用方 |
| 用量计量 | `usage_billing` | 用量记录 | 记录编号、调用方、API名称 |
| 缓存分层 | `cache_layer` | 缓存策略 | 策略编号、关联API、缓存层级 |
| 数据过滤 | `data_filter` | 过滤规则 | 规则编号、关联API、过滤字段 |
| 字段脱敏 | `field_mask` | 脱敏规则 | 规则编号、目标字段、脱敏算法 |
| SLA监控 | `sla_monitor` | SLA指标 | 指标编号、关联API、可用率目标 |
| 结果格式化 | `result_format` | 格式模板 | 模板编号、关联API、输出格式 |
| 查询优化 | `query_optimize` | 优化规则 | 规则编号、关联API、索引建议 |
| 异常归类 | `error_classify` | 异常类型 | 类型编号、类型名称、错误码段 |
| 数据归并 | `data_merge` | 归并规则 | 规则编号、主API、从API |
| 导出任务 | `export_task` | 导出任务 | 任务编号、关联API、查询参数 |
| 调用审计 | `call_audit` | 调用记录 | 记录编号、调用方、目标API |
| 版本兼容 | `version_compat` | 版本映射 | 映射编号、关联API、旧版本 |
| 批量查询 | `batch_query` | 批量任务 | 任务编号、关联API、查询清单 |
| SDK配置 | `sdk_config` | SDK配置 | 配置编号、关联API、SDK语言 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 审计写入身份

导出完成事件和普通调用事件都会写 `data/audit/<审计号>.json`。写入前先
通过 `reserve_audit` 在 `data/audit/_claims/` 下独占一个编号；候选编号
被占用时顺延，原审计号继续指向原记录。没有占位的一方不会被另一方的
提交认领：

- 导出已完成、普通调用尚未完成：支持人员仍可按原审计号复核导出，
  普通调用恢复后在它自己的编号上落盘；
- 普通调用已完成、导出尚未完成：调用记录保持完整，导出需要重新登记
  （暂存过完整结果的导出会在重启时自动补写）。

两类事件的附带产物分开存放：

| 内容 | 落盘位置 |
| --- | --- |
| 导出完成证据 | `data/audit/export_evidence/<审计号>.json` |
| 调用请求与响应 | `data/audit/calls/<审计号>.json` |
| 导出成品 | `data/exports/<导出任务id>.json|.csv` |

相关入口：

- `GET /api/call_audit/query`：调用审计查询，返回请求、响应与生产来源；
- `GET /api/call_audit/{audit_id}/audit-log`：按审计号读取单条记录；
- `GET /api/export_task/audit-log?audit_id=`：按审计号读导出日志，行数与下载凭据一致；
- `GET /api/export_task/{export_id}/audit`：按导出任务读取其完成事件；
- `POST /api/call_audit/recover`：恢复未完成审计写入（服务启动时也会自动执行）。


领域：数据工程
子方向：数据分析与数据应用

## 审计身份回归验证

`backend/tests/test_audit_identity.py` 覆盖“3 行导出 + 返回 1 行普通调用
在同一候选号窗口交错写盘，再恢复未完成写入”的场景，并分别沿原审计号、
调用记录编号和导出成品编号核对行数与证据。环境里未安装 pytest 时可直接
按函数顺序执行，或安装 pytest 后运行：

```bash
cd backend && .venv/bin/pip install pytest && .venv/bin/python -m pytest tests/test_audit_identity.py -q
```
