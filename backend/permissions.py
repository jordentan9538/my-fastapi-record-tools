from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class PermissionDefinition:
    key: str
    label: str
    category: str
    action: str  # "view" or "operate"
    description: str = ""


PERMISSIONS: List[PermissionDefinition] = [
    PermissionDefinition(
        key="customer.view",
        label="查看顾客资料",
        category="顾客",
        action="view",
        description="允许浏览顾客列表、详情、总览以及相关统计。",
    ),
    PermissionDefinition(
        key="customer.manage",
        label="管理顾客资料",
        category="顾客",
        action="operate",
        description="允许创建或更新顾客资料、上传照片与调整余额。",
    ),
    PermissionDefinition(
        key="loan.view",
        label="查看借款",
        category="借款",
        action="view",
        description="允许查看借款列表、详情与相关报表。",
    ),
    PermissionDefinition(
        key="loan.manage",
        label="管理借款",
        category="借款",
        action="operate",
        description="允许创建、编辑或删除借款记录。",
    ),
    PermissionDefinition(
        key="repayment.view",
        label="查看还款",
        category="还款",
        action="view",
        description="允许查看还款列表与详情。",
    ),
    PermissionDefinition(
        key="repayment.manage",
        label="管理还款",
        category="还款",
        action="operate",
        description="允许创建、编辑或删除还款记录。",
    ),
    PermissionDefinition(
        key="summary.view",
        label="访问概览面板",
        category="仪表盘",
        action="view",
        description="允许访问首页概览、实时统计及 WebSocket 推送。",
    ),
    PermissionDefinition(
        key="records.view",
        label="导出借还款记录",
        category="报表",
        action="view",
        description="允许按日期查询借还款记录与导出数据。",
    ),
    PermissionDefinition(
        key="reports.view",
        label="访问汇总报表",
        category="报表",
        action="view",
        description="允许查看整体汇总报表与经营指标。",
    ),
    PermissionDefinition(
        key="operationlog.view",
        label="查看操作日志",
        category="系统",
        action="view",
        description="允许查看操作日志、审计记录等。",
    ),
    PermissionDefinition(
        key="admin.manage",
        label="管理员管理",
        category="系统",
        action="operate",
        description="允许创建、编辑、启用或禁用后台账号以及配置权限。",
    ),
]

PERMISSION_MAP: Dict[str, PermissionDefinition] = {perm.key: perm for perm in PERMISSIONS}
ALL_PERMISSION_KEYS = set(PERMISSION_MAP.keys())


def sanitize_permissions(raw: Dict[str, bool] | None) -> Dict[str, bool]:
    if not raw:
        return {}
    sanitized: Dict[str, bool] = {}
    for key, value in raw.items():
        if key in ALL_PERMISSION_KEYS:
            sanitized[key] = bool(value)
    return sanitized