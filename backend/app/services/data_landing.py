"""数据落库服务：写入过程记录、冲突行处理、落库水位推进。

记录采集批次到目标存储的写入过程、冲突行处理和落库水位，
作为下游消费的唯一入口。水位单调递增，支持增量消费。
落库按映射生效清单转换采集记录：每个目标字段只采用清单中唯一
胜出的映射版本，并把各列版本快照写入落库记录，供核对与复算。
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


class DataLandingService(BaseModuleService):
    MODULE = "data_landing"
    ENTITY = "落库记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "落库编号", "关联批次", "目标存储", "写入模式", "写入行数",
        "冲突行数", "落库水位", "完成时间",
    ]
    REQUIRED_FIELDS = ["落库编号", "关联批次", "目标存储"]
    STATUS_ORDER = ["待写入", "写入中", "已写入", "写入异常"]
    ACTION_RULES = {"启动写入": "写入中", "确认写入": "已写入", "标记异常": "写入异常"}
    NEGATIVE_ACTIONS = []

    def advance_watermark(self, landing_id: int, watermark: int) -> dict[str, Any]:
        """推进落库水位（仅当新水位大于当前水位时生效）。"""
        entry = self.get_entry(landing_id)
        if entry is None:
            return {"ok": False, "reason": "落库记录不存在"}
        current = int(entry.get("落库水位", "0") or "0")
        if watermark > current:
            entry["落库水位"] = str(watermark)
            return {"ok": True, "landing_id": landing_id, "new_watermark": watermark}
        return {"ok": False, "reason": f"新水位{watermark}不大于当前水位{current}"}

    def get_watermark(self, target_storage: str) -> int:
        """获取指定目标存储的当前最高水位。"""
        items, _ = self.list_entries(size=10000)
        max_wm = 0
        for it in items:
            if it.get("目标存储") == target_storage and it.get("status") == "已写入":
                wm = int(it.get("落库水位", "0") or "0")
                if wm > max_wm:
                    max_wm = wm
        return max_wm

    def handle_conflicts(self, landing_id: int, conflict_count: int) -> dict[str, Any]:
        """处理冲突行：冲突行数>0则标记写入异常，否则确认写入。"""
        entry = self.get_entry(landing_id)
        if entry is None:
            return {"ok": False, "reason": "落库记录不存在"}
        if conflict_count > 0:
            self.run_action(landing_id, "标记异常")
            return {
                "ok": True,
                "landing_id": landing_id,
                "conflicts": conflict_count,
                "handled_as": "异常标记",
            }
        self.run_action(landing_id, "确认写入")
        return {"ok": True, "landing_id": landing_id, "conflicts": 0, "handled_as": "已确认"}

    # ── 按生效清单落库 ─────────────────────────────────────────

    def _manifest_winners(self, manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """清单中每个目标字段唯一胜出的映射（同列绝不会有两条生效）。"""
        winners: dict[str, dict[str, Any]] = {}
        for item in manifest.get("明细", []):
            if item.get("裁决") == "胜出":
                winners[str(item.get("目标字段"))] = item
        return winners

    def land_with_manifest(self, batch_id: int, records: list[dict[str, Any]],
                           manifest_id: int | None = None) -> dict[str, Any]:
        """按生效清单把已确认批次的采集记录转换落库。

        每个目标字段只采用清单中唯一胜出的映射版本转换；各列版本快照
        随落库记录保存，与生效清单逐列对得上。
        """
        from app.services import service_for  # 延迟导入避免循环依赖

        batch_svc = service_for("capture_batch")
        batch = batch_svc.get_entry(batch_id)
        if batch is None:
            return {"ok": False, "reason": "采集批次不存在"}
        if batch.get("status") != "已确认":
            return {"ok": False, "reason": f"批次状态为{batch.get('status')}，仅已确认批次可落库"}

        activation_svc = service_for("mapping_activation")
        if manifest_id is not None:
            manifest = activation_svc.get_entry(manifest_id)
            if manifest is None:
                return {"ok": False, "reason": "生效清单不存在"}
        else:
            manifest = activation_svc.latest_active()
            if manifest is None:
                return {"ok": False, "reason": "没有已生效的映射清单，不能落库"}
        if manifest.get("status") != "已生效":
            return {"ok": False, "reason": f"清单{manifest.get('清单编号')}尚未生效完成"}

        mapping_svc = service_for("field_mapping")
        winners = self._manifest_winners(manifest)
        if not winners:
            return {"ok": False, "reason": "生效清单没有胜出映射，不能落库"}

        # 校验胜出映射确已实际生效，且版本与清单一致
        plan: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        for field, item in winners.items():
            mapping = mapping_svc.get_entry(int(item["mapping_id"]))
            if mapping is None or mapping.get("status") != "已生效":
                return {"ok": False, "reason": f"映射{item.get('映射编号')}未实际生效，拒绝落库"}
            if str(mapping.get("映射版本")) != str(item.get("生效版本")):
                return {"ok": False, "reason": f"映射{item.get('映射编号')}版本与清单不一致"}
            plan[field] = (mapping, item)

        # 逐条转换：每个目标列只应用一次胜出转换
        transformed: list[dict[str, Any]] = []
        for record in records:
            row: dict[str, Any] = {}
            for field, (mapping, _item) in plan.items():
                raw = record.get(mapping.get("来源字段", ""), "")
                if (raw is None or str(raw) == "") and mapping.get("空值策略") == "skip":
                    continue
                row[field] = mapping_svc.apply_transform(mapping.get("转换函数", "strip"), raw)
            transformed.append(row)

        column_versions = {
            field: {
                "mapping_id": item["mapping_id"],
                "映射编号": item["映射编号"],
                "映射版本": item["生效版本"],
            }
            for field, (_mapping, item) in plan.items()
        }
        landing_values = {
            "落库编号": f"LAND-B{batch_id}",
            "关联批次": batch.get("批次编号", ""),
            "目标存储": "datalake_rpa_collect",
            "写入模式": "upsert",
            "写入行数": str(len(transformed)),
            "冲突行数": "0",
            "落库水位": str(int(time.time())),
            "完成时间": _now_iso(),
        }
        landing, _ = self.create_entry(landing_values)
        landing["生效清单"] = manifest.get("清单编号")
        landing["列版本快照"] = column_versions
        landing["_写入列"] = transformed
        self.run_action(int(landing["id"]), "启动写入")
        self.run_action(int(landing["id"]), "确认写入")
        return {
            "ok": True,
            "landing_id": landing["id"],
            "落库编号": landing.get("落库编号"),
            "生效清单": manifest.get("清单编号"),
            "写入行数": len(transformed),
            "列版本快照": column_versions,
            "rows": transformed,
        }

    def verify_landing_manifest(self, landing_id: int) -> dict[str, Any]:
        """核对落库记录的各列版本与关联生效清单是否一致。"""
        from app.services import service_for

        landing = self.get_entry(landing_id)
        if landing is None:
            return {"ok": False, "reason": "落库记录不存在"}
        manifest_no = landing.get("生效清单")
        if not manifest_no:
            return {"ok": False, "reason": "落库记录未关联生效清单"}
        activation_svc = service_for("mapping_activation")
        manifests, _ = activation_svc.list_entries(size=10000)
        manifest = next((m for m in manifests if m.get("清单编号") == manifest_no), None)
        if manifest is None:
            return {"ok": False, "reason": f"生效清单{manifest_no}不存在"}
        winners = self._manifest_winners(manifest)
        snapshot = landing.get("列版本快照", {})
        mismatches: list[str] = []
        for field, item in winners.items():
            snap = snapshot.get(field)
            if (snap is None
                    or str(snap.get("映射编号")) != str(item.get("映射编号"))
                    or str(snap.get("映射版本")) != str(item.get("生效版本"))):
                mismatches.append(field)
        extra = [field for field in snapshot if field not in winners]
        return {
            "ok": not mismatches and not extra,
            "landing_id": landing_id,
            "清单编号": manifest_no,
            "版本不符列": mismatches,
            "清单外列": extra,
        }

    def recompute_landing(self, landing_id: int,
                          records: list[dict[str, Any]]) -> dict[str, Any]:
        """按落库时的列版本快照复算各列：映射后续升版不影响历史批次。"""
        from app.services import service_for

        landing = self.get_entry(landing_id)
        if landing is None:
            return {"ok": False, "reason": "落库记录不存在"}
        snapshot = landing.get("列版本快照") or {}
        if not snapshot:
            return {"ok": False, "reason": "落库记录缺少列版本快照，无法复算"}
        mapping_svc = service_for("field_mapping")
        rows: list[dict[str, Any]] = []
        for record in records:
            row: dict[str, Any] = {}
            for field, meta in snapshot.items():
                mapping = mapping_svc.get_entry(int(meta["mapping_id"]))
                if mapping is None:
                    return {"ok": False, "reason": f"映射{meta.get('映射编号')}已不存在"}
                config = (mapping.get("_versions") or {}).get(str(meta["映射版本"]))
                if config is None:
                    return {"ok": False,
                            "reason": f"映射{meta.get('映射编号')}缺少历史版本{meta.get('映射版本')}配置"}
                raw = record.get(config.get("来源字段", ""), "")
                if (raw is None or str(raw) == "") and config.get("空值策略") == "skip":
                    continue
                row[field] = mapping_svc.apply_transform(config.get("转换函数", "strip"), raw)
            rows.append(row)
        return {
            "ok": True,
            "landing_id": landing_id,
            "采用版本": {field: meta["映射版本"] for field, meta in snapshot.items()},
            "rows": rows,
        }