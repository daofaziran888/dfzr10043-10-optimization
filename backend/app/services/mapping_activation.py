"""映射生效清单服务：批量生效的核对清单与冲突裁决留痕。

每次字段映射批量提交生成一份生效清单，记录每条映射的试算输入输出、
同列冲突的裁决结果和最终生效版本，供数据落库逐列核对。
清单按提交指纹去重：重复提交、中断重试都只产生一份生效记录。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


class MappingActivationService(BaseModuleService):
    MODULE = "mapping_activation"
    ENTITY = "生效清单"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "清单编号", "提交指纹", "提交映射数", "生效映射数", "冲突数", "生成时间",
    ]
    REQUIRED_FIELDS = ["清单编号"]
    STATUS_ORDER = ["生效中", "已生效", "已作废"]
    ACTION_RULES = {"作废清单": "已作废"}
    NEGATIVE_ACTIONS = ["作废清单"]

    def create_manifest(self, fingerprint: str, items: list[dict[str, Any]],
                        conflict_count: int) -> dict[str, Any]:
        """登记一份生效清单（先生成后应用，中断后可凭提交指纹续作）。"""
        rows = store.rows(self.MODULE)
        manifest_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        manifest = {
            "id": manifest_id,
            "清单编号": f"ACTV-{manifest_id:04d}",
            "提交指纹": fingerprint,
            "提交映射数": len(items),
            "生效映射数": 0,
            "冲突数": conflict_count,
            "生成时间": _now_iso(),
            "status": "生效中",
            "pending": True,
            "abnormal": False,
            "明细": items,
        }
        rows.append(manifest)
        return manifest

    def find_by_fingerprint(self, fingerprint: str) -> dict[str, Any] | None:
        """按提交指纹查找既有清单（取最新一份），用于重复提交去重与中断续作。"""
        matches = [
            row for row in store.rows(self.MODULE)
            if row.get("提交指纹") == fingerprint
        ]
        if not matches:
            return None
        return max(matches, key=lambda row: int(row.get("id", 0)))

    def latest_active(self) -> dict[str, Any] | None:
        """最新一份已生效清单，落库默认按它转换记录。"""
        active = [
            row for row in store.rows(self.MODULE)
            if row.get("status") == "已生效"
        ]
        if not active:
            return None
        return max(active, key=lambda row: int(row.get("id", 0)))

    def conflict_report(self, manifest_id: int) -> dict[str, Any]:
        """汇总清单中同列冲突的来源映射与各自的试算结果。"""
        manifest = self.get_entry(manifest_id)
        if manifest is None:
            return {"ok": False, "reason": "生效清单不存在"}
        groups: dict[str, list[dict[str, Any]]] = {}
        for item in manifest.get("明细", []):
            groups.setdefault(str(item.get("目标字段", "")), []).append(item)
        conflicts: dict[str, list[dict[str, Any]]] = {}
        for field, items in groups.items():
            if not any(item.get("裁决") == "冲突落选" for item in items):
                continue
            conflicts[field] = [
                {
                    "映射编号": item.get("映射编号"),
                    "来源字段": item.get("来源字段"),
                    "转换函数": item.get("转换函数"),
                    "试算结果": item.get("试算结果"),
                    "裁决": item.get("裁决"),
                    "落选原因": item.get("落选原因", ""),
                }
                for item in items
            ]
        return {
            "ok": True,
            "清单编号": manifest.get("清单编号"),
            "冲突字段数": len(conflicts),
            "冲突明细": conflicts,
        }
