"""字段映射服务：映射规则管理、批量试算生效、同列冲突裁决与版本快照。

维护采集记录到目标存储字段的映射、转换函数和空值策略。
批量生效时按实际采集记录逐条试算，同一目标字段被多条规则覆盖时
裁决出唯一胜出者（版本高者优先，同版本映射编号小者优先），并生成
生效清单供数据落库逐列核对。提交按内容指纹去重：重复提交只生效一次，
中断后重试凭指纹续作，不会产生第二份生效记录。每次生效把映射配置
按版本快照留存，历史已确认批次可随时按当时的版本复算各列。
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.dedup import sha256_hex
from app.services.base import BaseModuleService
from app.store import store


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


class FieldMappingService(BaseModuleService):
    MODULE = "field_mapping"
    ENTITY = "映射规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "映射编号", "来源字段", "目标字段", "转换函数", "空值策略",
        "类型声明", "映射版本", "最近生效",
    ]
    REQUIRED_FIELDS = ["映射编号", "来源字段", "目标字段"]
    STATUS_ORDER = ["草稿", "试算中", "已生效", "已作废"]
    ACTION_RULES = {"提交试算": "试算中", "生效映射": "已生效", "作废映射": "已作废"}
    NEGATIVE_ACTIONS = ["作废映射"]

    _TRANSFORM_FUNCTIONS: dict[str, Callable[[str], Any]] = {
        "str_to_int": lambda v: int(v) if v else 0,
        "str_to_float": lambda v: float(v) if v else 0.0,
        "to_upper": lambda v: str(v).upper(),
        "to_lower": lambda v: str(v).lower(),
        "strip": lambda v: str(v).strip(),
        "default_empty": lambda v: str(v) if v else "",
        "zero_pad": lambda v: str(v).strip().zfill(3),
    }

    # ── 单条试算与转换 ─────────────────────────────────────────

    def apply_transform(self, transform: str, value: Any) -> Any:
        """按转换函数名转换单个取值，落库与复算共用同一入口。"""
        fn = self._TRANSFORM_FUNCTIONS.get(transform, self._TRANSFORM_FUNCTIONS["strip"])
        return fn(str(value))

    def trial_mapping(self, mapping_id: int, sample_value: str) -> dict[str, Any]:
        """对单条映射做试算，返回转换结果供比对。"""
        entry = self.get_entry(mapping_id)
        if entry is None:
            return {"ok": False, "reason": "映射规则不存在"}
        transform = entry.get("转换函数", "strip")
        null_policy = entry.get("空值策略", "skip")
        if not sample_value and null_policy == "skip":
            return {"ok": False, "reason": "空值且策略为skip，跳过"}
        fn = self._TRANSFORM_FUNCTIONS.get(transform, self._TRANSFORM_FUNCTIONS["strip"])
        try:
            result = fn(sample_value)
            return {"ok": True, "mapping_id": mapping_id, "input": sample_value, "output": result}
        except (ValueError, TypeError) as e:
            return {"ok": False, "reason": f"转换失败: {e}"}

    # ── 版本快照 ───────────────────────────────────────────────

    @staticmethod
    def _config_of(entry: dict[str, Any]) -> dict[str, Any]:
        """映射的版本化配置：同一版本号下配置变化时自动升版。"""
        return {
            "来源字段": entry.get("来源字段", ""),
            "目标字段": entry.get("目标字段", ""),
            "转换函数": entry.get("转换函数", ""),
            "空值策略": entry.get("空值策略", ""),
            "类型声明": entry.get("类型声明", ""),
        }

    @staticmethod
    def _bump_version(label: str) -> str:
        match = re.search(r"(\d+)$", label)
        if match:
            return f"{label[:match.start()]}{int(match.group(1)) + 1}"
        return f"{label}-v2"

    def _ensure_version(self, entry: dict[str, Any]) -> str:
        """定版：配置首次出现则按当前版本号快照；同号配置已变则升版。

        幂等：相同配置重复定版得到相同版本号，中断重试不会重复升版。
        """
        versions = entry.setdefault("_versions", {})
        label = str(entry.get("映射版本") or "v1")
        config = self._config_of(entry)
        if label not in versions:
            versions[label] = config
            return label
        if versions[label] != config:
            new_label = self._bump_version(label)
            while new_label in versions:
                new_label = self._bump_version(new_label)
            versions[new_label] = config
            entry["映射版本"] = new_label
            return new_label
        return label

    @staticmethod
    def _version_rank(entry: dict[str, Any]) -> int:
        match = re.search(r"(\d+)$", str(entry.get("映射版本") or ""))
        return int(match.group(1)) if match else 0

    # ── 批量生效 ───────────────────────────────────────────────

    def _trial_against_records(self, entry: dict[str, Any],
                               records: list[dict[str, Any]]) -> dict[str, Any]:
        """按实际采集记录逐条试算一条映射，任一记录转换异常即试算失败。"""
        transform = entry.get("转换函数") or "strip"
        null_policy = entry.get("空值策略") or "skip"
        fn = self._TRANSFORM_FUNCTIONS.get(transform)
        if fn is None:
            return {"outcome": "failed", "reason": f"未知转换函数:{transform}", "details": []}
        source = entry.get("来源字段", "")
        details: list[dict[str, Any]] = []
        for idx, record in enumerate(records):
            raw = record.get(source, "")
            if raw is None or str(raw) == "":
                if null_policy == "skip":
                    details.append({"记录序号": idx, "输入": raw, "跳过": True})
                    continue
                raw = ""
            try:
                output = fn(str(raw))
            except (ValueError, TypeError) as exc:
                return {
                    "outcome": "failed",
                    "reason": f"第{idx + 1}条记录转换失败: {exc}",
                    "details": details,
                }
            details.append({"记录序号": idx, "输入": raw, "输出": output})
        outputs = [d["输出"] for d in details if "输出" in d]
        if not outputs:
            return {"outcome": "skipped", "reason": "空值且策略为skip，全部跳过", "details": details}
        return {"outcome": "ok", "outputs": outputs, "details": details}

    def _submission_fingerprint(self, mapping_ids: list[int],
                                submission_id: str | None) -> tuple[str, list[dict[str, Any]], list[int]]:
        """提交指纹：由映射内容决定，重复提交（内容不变）指纹相同。"""
        entries: list[dict[str, Any]] = []
        missing: list[int] = []
        for mid in dict.fromkeys(mapping_ids):
            entry = self.get_entry(mid)
            if entry is None:
                missing.append(mid)
            else:
                entries.append(entry)
        entries.sort(key=lambda e: int(e.get("id", 0)))
        payload = "|".join(
            f"{e.get('id')}:{e.get('来源字段')}:{e.get('目标字段')}"
            f":{e.get('转换函数')}:{e.get('空值策略')}"
            for e in entries
        )
        if submission_id:
            payload = f"{submission_id}#{payload}"
        return sha256_hex(payload), entries, missing

    def _plan_items(self, entries: list[dict[str, Any]], missing: list[int],
                    records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
        """试算并裁决：同列多条规则结果不同记冲突，裁决唯一胜出者。"""
        items: list[dict[str, Any]] = []
        candidates: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for entry in entries:
            trial = self._trial_against_records(entry, records)
            item: dict[str, Any] = {
                "mapping_id": entry.get("id"),
                "映射编号": entry.get("映射编号"),
                "来源字段": entry.get("来源字段"),
                "目标字段": entry.get("目标字段"),
                "转换函数": entry.get("转换函数"),
                "试算明细": trial.get("details", []),
                "试算结果": trial.get("outputs", []),
            }
            if trial["outcome"] == "ok":
                candidates.append((entry, trial, item))
            else:
                item["裁决"] = "试算失败" if trial["outcome"] == "failed" else "试算跳过"
                item["落选原因"] = trial.get("reason", "")
            items.append(item)
        for mid in missing:
            items.append({"mapping_id": mid, "裁决": "映射不存在", "落选原因": "映射规则不存在"})

        groups: dict[str, list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]]] = {}
        for candidate in candidates:
            groups.setdefault(str(candidate[0].get("目标字段", "")), []).append(candidate)

        conflict_count = 0
        for field, group in groups.items():
            vectors = {repr(trial["outputs"]) for _, trial, _ in group}
            if len(vectors) > 1:
                conflict_count += 1
            # 裁决规则：版本号高者优先，同版本映射编号小者优先
            ranked = sorted(
                group,
                key=lambda c: (-self._version_rank(c[0]), int(c[0].get("id", 0))),
            )
            winner_entry, winner_trial, winner_item = ranked[0]
            winner_item["裁决"] = "胜出"
            winner_item["生效版本"] = self._ensure_version(winner_entry)
            for entry, trial, item in ranked[1:]:
                same_vector = repr(trial["outputs"]) == repr(winner_trial["outputs"])
                item["裁决"] = "冗余落选" if same_vector else "冲突落选"
                item["落选原因"] = (
                    f"与{winner_entry.get('映射编号')}同列（{field}）"
                    + ("结果一致，作为冗余规则落选" if same_vector
                       else f"结果{trial['outputs']}与胜出结果{winner_trial['outputs']}不同，裁决落选")
                )
        return items, conflict_count

    def _apply_winners(self, manifest: dict[str, Any]) -> list[int]:
        """应用清单中的胜出映射：定版、置为已生效、取代同列旧生效映射。

        全部按清单内容幂等回放，中断后重试不会产生第二次生效。
        """
        activated: list[int] = []
        for item in manifest.get("明细", []):
            if item.get("裁决") != "胜出":
                continue
            entry = self.get_entry(int(item["mapping_id"]))
            if entry is None:
                continue
            version = str(item["生效版本"])
            entry["映射版本"] = version
            entry.setdefault("_versions", {})[version] = self._config_of(entry)
            if entry.get("status") != "已生效":
                self.run_action(int(entry["id"]), "提交试算")
                self.run_action(int(entry["id"]), "生效映射")
            entry["最近生效"] = _now_iso()
            superseded: list[str] = []
            for other in store.rows(self.MODULE):
                if (int(other.get("id", 0)) != int(entry["id"])
                        and other.get("目标字段") == entry.get("目标字段")
                        and other.get("status") == "已生效"):
                    other["status"] = "已作废"
                    other["pending"] = False
                    other["_superseded_by"] = entry.get("映射编号")
                    superseded.append(str(other.get("映射编号")))
            item["取代映射"] = superseded
            activated.append(int(entry["id"]))
        return activated

    @staticmethod
    def _manifest_result(manifest: dict[str, Any], *, deduplicated: bool = False,
                         resumed: bool = False) -> dict[str, Any]:
        items = manifest.get("明细", [])
        return {
            "ok": True,
            "deduplicated": deduplicated,
            "resumed": resumed,
            "manifest_id": manifest.get("id"),
            "清单编号": manifest.get("清单编号"),
            "提交指纹": manifest.get("提交指纹"),
            "生效映射数": sum(1 for i in items if i.get("裁决") == "胜出"),
            "冲突数": manifest.get("冲突数", 0),
            "activated": [i["mapping_id"] for i in items if i.get("裁决") == "胜出"],
            "conflicts": [i for i in items if i.get("裁决") == "冲突落选"],
            "failed": [i for i in items if i.get("裁决") in ("试算失败", "映射不存在")],
            "skipped": [i for i in items if i.get("裁决") == "试算跳过"],
            "明细": items,
        }

    def batch_activate(self, mapping_ids: list[int],
                       records: list[dict[str, Any]] | None = None,
                       submission_id: str | None = None,
                       fail_hook: Callable[[dict[str, Any]], None] | None = None
                       ) -> dict[str, Any]:
        """批量生效映射规则：试算→冲突裁决→生成生效清单→幂等应用。

        - records：实际采集记录，逐条参与试算校验；
        - 提交指纹去重：内容相同的重复提交直接返回既有清单，只生效一次；
        - 中断续作：清单先生成（生效中），重试时凭指纹接续完成，
          生效记录仅有一份；fail_hook 用于注入中断（清单生成后回调）。
        试算失败只记录在清单中，不触碰任何已确认的历史数据。
        """
        from app.services import service_for  # 延迟导入避免循环依赖

        activation_svc = service_for("mapping_activation")
        records = list(records or [{}])
        fingerprint, entries, missing = self._submission_fingerprint(mapping_ids, submission_id)

        existing = activation_svc.find_by_fingerprint(fingerprint)
        if existing is not None and existing.get("status") == "已生效":
            return self._manifest_result(existing, deduplicated=True)

        if existing is not None and existing.get("status") == "生效中":
            manifest = existing  # 中断后的重试：沿用既有清单续作
            resumed = True
        else:
            items, conflict_count = self._plan_items(entries, missing, records)
            manifest = activation_svc.create_manifest(fingerprint, items, conflict_count)
            resumed = False

        if fail_hook is not None:
            fail_hook(manifest)

        self._apply_winners(manifest)
        manifest["生效映射数"] = sum(1 for i in manifest["明细"] if i.get("裁决") == "胜出")
        manifest["status"] = "已生效"
        manifest["pending"] = False
        return self._manifest_result(manifest, resumed=resumed)

    def activate_mappings(self, mapping_ids: list[int],
                          records: list[dict[str, Any]] | None = None,
                          submission_id: str | None = None) -> dict[str, Any]:
        """批量生效映射规则（经试算、冲突裁决与生效清单的完整流程）。"""
        return self.batch_activate(mapping_ids, records=records, submission_id=submission_id)
