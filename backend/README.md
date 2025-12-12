# 放贷记录后台服务

本目录提供一个基于 FastAPI + SQLModel 的放贷记录后台，现已固定依赖 MySQL（通过 `DATABASE_URL` 指定连接串）。系统支持通过 Web UI 或 REST API 手动录入顾客、借贷、还款信息，并提供概要统计。

## 功能概览

- 顾客、借贷、还款 CRUD 接口
- 汇总统计与按日期筛选 API
- 内置简易 Web 后台，可直接在浏览器中录入数据
- 自动 20% 月度复利预测，展示下一次结息时间
- 借贷创建时可录入并自动扣除手续费，移除还款端手续费输入
- FastAPI 文档页：`/docs`
- 采用 MySQL 持久化，可部署到任意支持该数据库的平台（Render、Railway、Fly.io 等）；若你手上仍有旧的 SQLite 数据，可使用迁移脚本一次性导入
- 自带 Pytest 测试，确保核心流程可用

## 快速运行

```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn backend.app:app --reload --port 8000
```

启动后访问：
- Web 后台：http://127.0.0.1:8000/
- API 文档：http://127.0.0.1:8000/docs

## 部署到 GitHub + Render 示例

1. **创建仓库**：将整个项目推送到 GitHub，例如 `yourname/loan-record-backend`。
2. **后端部署（Render）**：
   - 新建 Web Service，连接上述仓库。
   - 运行命令：`uvicorn backend.app:app --host 0.0.0.0 --port 10000`
   - 指定 Python 版本 3.11，自动安装 `backend/requirements.txt`。
   - 部署完成后获得后端 URL，如 `https://loan-record.onrender.com`。
3. **前端链接（GitHub Pages 可选）**：
   - 在仓库中新建 `frontend/index.html`（可参考本项目的静态页面示例）。
   - GitHub 仓库设置 → Pages → Source 选择 `main` 分支 + `/docs` 或 `root`。
   - 将后端 URL 写入前端脚本中的 `API_BASE_URL`，即可通过 GitHub Pages 链接访问后台界面。

> 部署完成后，你可以把 GitHub Pages URL 分享给团队，点击即可通过浏览器访问，所有数据写入后端数据库。

## 配置项

- `DATABASE_URL`：**必须**设置为 `mysql+pymysql://user:pass@host:3306/loan_records?charset=utf8mb4` 形式的连接串，应用才能启动。
- `CORS`：当前允许所有来源，生产环境可改为白名单。

## 迁移到 MySQL

1. **准备环境**
   - 在目标 MySQL 中创建数据库（推荐 `utf8mb4`）。
   - 安装依赖：`pip install -r backend/requirements.txt`（已包含 `PyMySQL`）。
2. **执行迁移脚本**

```bash
cd backend
python -m backend.scripts.migrate_sqlite_to_mysql \
  --mysql-url "mysql+pymysql://user:pass@host:3306/loan_records?charset=utf8mb4" \
  --reset-destination  # 如果需要清空目标库
```

脚本会从默认的 `backend/data/loan_records.db` 读取全部数据，按依赖顺序写入 MySQL。若你的 SQLite 文件在其他路径，可加 `--sqlite-path /path/to/db.sqlite`。

3. **切换运行时数据库**
   - 在部署或本地运行时设置 `DATABASE_URL` 指向上面的 MySQL 连接串。
   - 重新启动 `uvicorn backend.app:app --reload` 即可使用 MySQL。
   - 注意：SQLite 专用的自动补字段逻辑已移除，所有库结构需通过 SQLModel 的迁移/初始化或手动工具保证一致。

## 测试

```bash
cd backend
pytest
```

## 身份验证与权限

- **后台登录**：访问 `/auth/login` 使用管理员或客服类账号登录，所有页面与 API 都通过 Session Cookie (`session_token`) 保护。启动时会自动生成 `admin/admin123`，请立即修改密码或通过 `BACKEND_ADMIN_USERNAME/BACKEND_ADMIN_PASSWORD` 环境变量覆盖。
- **管理员管理**：登录主控制台后，在“管理员管理”模块即可创建管理员、客服（`cs`）、财务（`account`）等后台账号，逐项勾选“可见/可操作”权限，或重置密码、切换启用状态。该模块使用以下 API：
   - `GET /api/admin/users`：列出所有后台/顾客账号，包含 `permissions_json`。
   - `POST /api/admin/users`：创建账号，body 支持 `username`、`password`、`role`、`permissions`、可选 `customer_id`（仅当 `role=customer` 时有效）。
   - `PUT /api/admin/users/{id}`：更新角色、权限勾选、用户名与启用状态。
   - `POST /api/admin/users/{id}/status`：单独切换 `is_active`。
   - `POST /api/admin/users/{id}/reset-password`：为该用户设置新密码。
   - `GET /api/admin/permissions`：前端使用的权限目录（分组、操作类型、描述），用于渲染勾选矩阵。
- **顾客登录（JWT）**：顾客端调用 `POST /customer/auth/login` 获取 `access_token`（可选 `refresh_token`），之后向 `/customer/api/*` 传递 `Authorization: Bearer <token>` 即可查询个人资料、借贷、还款、汇总与复利流水。
- **新环境变量**：
   - `BACKEND_SECRET_KEY`、`BACKEND_JWT_ALGORITHM`：JWT 与 Session 签名。
   - `BACKEND_ACCESS_TOKEN_MINUTES`、`BACKEND_REFRESH_TOKEN_MINUTES`：token 过期时间。
   - `BACKEND_SESSION_HOURS`、`BACKEND_SESSION_COOKIE_*`：Session Cookie 设置。
   - `BACKEND_ADMIN_USERNAME`、`BACKEND_ADMIN_PASSWORD`：首次启动自动生成的管理员账号。

## 数据一致性审计

提供 `backend/scripts/audit_consistency.py` 用于离线巡检数据库中的客户、借贷、还款与复利事件是否一致：

```bash
cd backend
python -m backend.scripts.audit_consistency
```

常用参数：

- `--tolerance 0.05`：允许的金额误差（默认 0.01）。
- `--json`：以 JSON 格式输出，便于 CI 或其他脚本解析。

脚本会检查：

- 顾客与借贷编号是否缺失/重复。
- 还款/借贷引用是否合法、金额是否为正。
- `last_principal`、`projected_balance` 是否与重新计算的余额或最后一次 `CompoundBalanceEvent` 对齐。
- 复利事件中是否出现负数余额。

## 与现有 Excel 工具联动

- 保持 `Record.py` 作为离线备份/批量导入工具。
- 可编写同步脚本：读取 Excel 后调用后端 API 批量写入。
- 建议定期调用 `/api/summary`，下载 JSON 后与 Excel 做比对，确保数据一致。
