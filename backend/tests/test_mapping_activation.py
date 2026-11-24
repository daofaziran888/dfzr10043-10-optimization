"""字段映射批量生效链路测试：试算、同列冲突裁决、生效清单与落库核对。

场景：一次提交三条草稿映射，其中两条指向同一数字列、转换结果不同
（7 与 007），另一条处理独立目标字段；验证冲突可查询、重复提交与
中断重试只生效一次、落库各列版本与清单一致、历史批次可按当时版本复算。
"""
from __future__ import annotations

import copy

import pytest

from app.services import service_for
from app.store import store

RECORDS = [{"amount_raw": "007", "name_raw": "  Alice  "}]


def _make_drafts() -> dict[str, int]:
    """登记三条草稿：两条同列数字转换（7/007），一条独立列。"""
    mapping_svc = service_for("field_mapping")
    drafts = {}
    int_mapping, _ = mapping_svc.create_entry({
        "映射编号": "FIEL-T01", "来源字段": "amount_raw", "目标字段": "amount",
        "转换函数": "str_to_int", "空值策略": "skip", "类型声明": "int",
        "映射版本": "v1", "最近生效": "",
    })
    pad_mapping, _ = mapping_svc.create_entry({
        "映射编号": "FIEL-T02", "来源字段": "amount_raw", "目标字段": "amount",
        "转换函数": "zero_pad", "空值策略": "skip", "类型声明": "string",
        "映射版本": "v1", "最近生效": "",
    })
    name_mapping, _ = mapping_svc.create_entry({
        "映射编号": "FIEL-T03", "来源字段": "name_raw", "目标字段": "name",
        "转换函数": "strip", "空值策略": "skip", "类型声明": "string",
        "映射版本": "v1", "最近生效": "",
    })
    drafts["int"] = int_mapping["id"]
    drafts["pad"] = pad_mapping["id"]
    drafts["name"] = name_mapping["id"]
    return drafts


def _manifest_rows() -> list[dict]:
    return store.rows("mapping_activation")


def _confirmed_batch() -> int:
    batch_svc = service_for("capture_batch")
    batch, _ = batch_svc.create_entry({
        "批次编号": "CAPT-T01", "关联执行": "FLOW-T01", "记录行数": "1",
        "采集窗口": "[2026-11-24]", "完整性校验": "sha256_t01",
        "批次标签": "test", "上游来源": "test-source", "生成时间": "2026-11-24",
    })
    batch_svc.run_action(batch["id"], "启动校验")
    batch_svc.run_action(batch["id"], "确认批次")
    return batch["id"]


class TestBatchActivationTrialAndConflict:
    """三条草稿试算成 7、007 与另一列有效取值，同列冲突可查询。"""

    def test_trial_results_and_conflict_resolution(self):
        mapping_svc = service_for("field_mapping")
        ids = _make_drafts()
        result = mapping_svc.batch_activate(
            [ids["int"], ids["pad"], ids["name"]], records=RECORDS)

        assert result["ok"] is True
        assert result["生效映射数"] == 2
        assert result["冲突数"] == 1

        items = {item["映射编号"]: item for item in result["明细"]}
        # 数字列：str_to_int 试算成 7，zero_pad 试算成 007
        assert items["FIEL-T01"]["试算结果"] == [7]
        assert items["FIEL-T02"]["试算结果"] == ["007"]
        # 独立列：strip 得到有效取值
        assert items["FIEL-T03"]["试算结果"] == ["Alice"]
        # 裁决：同版本编号小者胜出，另一条冲突落选，独立列胜出
        assert items["FIEL-T01"]["裁决"] == "胜出"
        assert items["FIEL-T02"]["裁决"] == "冲突落选"
        assert "FIEL-T01" in items["FIEL-T02"]["落选原因"]
        assert items["FIEL-T03"]["裁决"] == "胜出"
        # 只有胜出者实际生效
        assert mapping_svc.get_entry(ids["int"])["status"] == "已生效"
        assert mapping_svc.get_entry(ids["name"])["status"] == "已生效"
        assert mapping_svc.get_entry(ids["pad"])["status"] == "草稿"

    def test_conflict_report_shows_sources_and_results(self):
        mapping_svc = service_for("field_mapping")
        ids = _make_drafts()
        result = mapping_svc.batch_activate(
            [ids["int"], ids["pad"], ids["name"]], records=RECORDS)

        activation_svc = service_for("mapping_activation")
        report = activation_svc.conflict_report(result["manifest_id"])
        assert report["ok"] is True
        assert report["冲突字段数"] == 1
        amount_items = report["冲突明细"]["amount"]
        by_no = {item["映射编号"]: item for item in amount_items}
        # 冲突来源两条映射及各自试算结果都能查到
        assert by_no["FIEL-T01"]["试算结果"] == [7]
        assert by_no["FIEL-T01"]["裁决"] == "胜出"
        assert by_no["FIEL-T02"]["试算结果"] == ["007"]
        assert by_no["FIEL-T02"]["裁决"] == "冲突落选"
        # 独立列不在冲突报告中
        assert "name" not in report["冲突明细"]

    def test_manifest_queryable_via_resource_api(self):
        mapping_svc = service_for("field_mapping")
        ids = _make_drafts()
        result = mapping_svc.batch_activate(
            [ids["int"], ids["pad"], ids["name"]], records=RECORDS)

        activation_svc = service_for("mapping_activation")
        manifest = activation_svc.get_entry(result["manifest_id"])
        assert manifest["清单编号"] == result["清单编号"]
        assert manifest["status"] == "已生效"
        assert manifest["提交映射数"] == 3
        assert manifest["生效映射数"] == 2
        assert manifest["冲突数"] == 1
        # 字段试算结果写入批量生效清单
        outputs = {
            item["映射编号"]: item["试算结果"] for item in manifest["明细"]
        }
        assert outputs["FIEL-T01"] == [7]
        assert outputs["FIEL-T02"] == ["007"]
        assert outputs["FIEL-T03"] == ["Alice"]


class TestBatchActivationIdempotency:
    """重复提交只生效一次；中断并重试后生效记录仅有一次。"""

    def test_duplicate_submission_activates_once(self):
        mapping_svc = service_for("field_mapping")
        ids = _make_drafts()
        id_list = [ids["int"], ids["pad"], ids["name"]]
        first = mapping_svc.batch_activate(id_list, records=RECORDS)
        second = mapping_svc.batch_activate(id_list, records=RECORDS)

        assert first["deduplicated"] is False
        assert second["deduplicated"] is True
        assert second["清单编号"] == first["清单编号"]
        assert len(_manifest_rows()) == 1
        # 版本没有因重复提交而变动
        assert mapping_svc.get_entry(ids["int"])["映射版本"] == "v1"

    def test_interrupt_and_retry_keeps_single_activation(self):
        mapping_svc = service_for("field_mapping")
        ids = _make_drafts()
        id_list = [ids["int"], ids["pad"], ids["name"]]

        def crash(_manifest):
            raise RuntimeError("模拟生效中断")

        with pytest.raises(RuntimeError):
            mapping_svc.batch_activate(id_list, records=RECORDS, fail_hook=crash)
        # 中断后清单已生成但尚未生效完成
        assert len(_manifest_rows()) == 1
        assert _manifest_rows()[0]["status"] == "生效中"

        retried = mapping_svc.batch_activate(id_list, records=RECORDS)
        assert retried["resumed"] is True
        # 生效记录仅有一次
        assert len(_manifest_rows()) == 1
        assert _manifest_rows()[0]["status"] == "已生效"
        # 胜出映射只生效一次：版本快照不重复、状态已生效
        winner = mapping_svc.get_entry(ids["int"])
        assert winner["status"] == "已生效"
        assert winner["映射版本"] == "v1"
        assert list(winner["_versions"].keys()) == ["v1"]

    def test_trial_failure_does_not_touch_confirmed_history(self):
        mapping_svc = service_for("field_mapping")
        batch_id = _confirmed_batch()
        bad, _ = mapping_svc.create_entry({
            "映射编号": "FIEL-T09", "来源字段": "amount_raw", "目标字段": "amount",
            "转换函数": "str_to_int", "空值策略": "skip", "类型声明": "int",
            "映射版本": "v1", "最近生效": "",
        })
        history_before = {
            "capture_batch": copy.deepcopy(store.rows("capture_batch")),
            "data_landing": copy.deepcopy(store.rows("data_landing")),
        }
        result = mapping_svc.batch_activate(
            [bad["id"]], records=[{"amount_raw": "not_a_number"}])

        assert result["ok"] is True
        assert result["生效映射数"] == 0
        assert result["failed"][0]["裁决"] == "试算失败"
        assert "转换失败" in result["failed"][0]["落选原因"]
        # 试算失败的映射未生效
        failed_mapping = mapping_svc.get_entry(bad["id"])
        assert failed_mapping["status"] == "草稿"
        # 已确认的历史数据（批次、落库）原样未动
        assert store.rows("capture_batch") == history_before["capture_batch"]
        assert store.rows("data_landing") == history_before["data_landing"]
        assert service_for("capture_batch").get_entry(batch_id)["status"] == "已确认"


class TestLandingWithManifest:
    """落库使用实际生效的规则转换记录，各列版本与清单对得上。"""

    def _activate_and_batch(self) -> tuple[dict[str, int], int, dict]:
        mapping_svc = service_for("field_mapping")
        ids = _make_drafts()
        activation = mapping_svc.batch_activate(
            [ids["int"], ids["pad"], ids["name"]], records=RECORDS)
        batch_id = _confirmed_batch()
        return ids, batch_id, activation

    def test_landing_applies_single_numeric_transform(self):
        ids, batch_id, activation = self._activate_and_batch()
        landing_svc = service_for("data_landing")
        landed = landing_svc.land_with_manifest(batch_id, RECORDS)

        assert landed["ok"] is True
        assert landed["生效清单"] == activation["清单编号"]
        # 数字列只采用胜出的一条转换：落库为 7，绝不同时出现 007
        assert landed["rows"] == [{"amount": 7, "name": "Alice"}]
        assert landed["rows"][0]["amount"] != "007"
        # 每个目标字段使用唯一版本
        versions = landed["列版本快照"]
        assert versions["amount"]["映射编号"] == "FIEL-T01"
        assert versions["amount"]["映射版本"] == "v1"
        assert versions["name"]["映射编号"] == "FIEL-T03"
        # 落选映射不在列版本快照中
        assert all(v["映射编号"] != "FIEL-T02" for v in versions.values())

    def test_landing_versions_match_manifest(self):
        ids, batch_id, activation = self._activate_and_batch()
        landing_svc = service_for("data_landing")
        landed = landing_svc.land_with_manifest(batch_id, RECORDS)

        check = landing_svc.verify_landing_manifest(landed["landing_id"])
        assert check["ok"] is True
        assert check["版本不符列"] == []
        assert check["清单外列"] == []

    def test_landing_rejects_unconfirmed_batch(self):
        ids, batch_id, activation = self._activate_and_batch()
        batch_svc = service_for("capture_batch")
        batch, _ = batch_svc.create_entry({
            "批次编号": "CAPT-T02", "关联执行": "FLOW-T01", "记录行数": "1",
        })
        landing_svc = service_for("data_landing")
        result = landing_svc.land_with_manifest(batch["id"], RECORDS)
        assert result["ok"] is False
        assert "已确认" in result["reason"]

    def test_historical_batch_recomputes_with_original_version(self):
        ids, batch_id, activation = self._activate_and_batch()
        landing_svc = service_for("data_landing")
        mapping_svc = service_for("field_mapping")
        landed = landing_svc.land_with_manifest(batch_id, RECORDS)
        assert landed["rows"][0]["amount"] == 7

        # 胜出映射后续升版：同列改用 zero_pad，再次提交生效
        mapping = mapping_svc.get_entry(ids["int"])
        mapping["转换函数"] = "zero_pad"
        second = mapping_svc.batch_activate([ids["int"]], records=RECORDS)
        assert second["清单编号"] != activation["清单编号"]
        assert mapping_svc.get_entry(ids["int"])["映射版本"] == "v2"

        # 新落库按新版本转换为 007
        batch2 = _confirmed_batch()
        landed2 = landing_svc.land_with_manifest(batch2, RECORDS)
        assert landed2["rows"][0]["amount"] == "007"

        # 历史已确认批次仍按当时的 v1 复算出 7
        recomputed = landing_svc.recompute_landing(landed["landing_id"], RECORDS)
        assert recomputed["ok"] is True
        assert recomputed["采用版本"]["amount"] == "v1"
        assert recomputed["rows"][0]["amount"] == 7
        # 历史落库与当时清单的核对关系不受升版影响
        check = landing_svc.verify_landing_manifest(landed["landing_id"])
        assert check["ok"] is True


class TestPipelineLink2Activation:
    """链路2：字段映射经批量试算生效并生成生效清单。"""

    def test_collect_land_produces_activation_manifest(self):
        from app.pipeline import collect_files_and_land_data

        run_svc = service_for("flow_run")
        run_id = run_svc.list_entries(size=1000)[0][0]["id"]
        mapping_svc = service_for("field_mapping")
        mapping_ids = [m["id"] for m in mapping_svc.list_entries(size=1000)[0][:2]]
        result = collect_files_and_land_data(run_id, [
            {"文件名称": "output_page1.csv", "文件类型": "text/csv", "字节大小": "4096"},
        ], mapping_ids)
        assert result["status"] == "landed"
        assert result["activation_manifest"] is not None
        activation_svc = service_for("mapping_activation")
        manifests = [
            row for row in store.rows("mapping_activation")
            if row.get("清单编号") == result["activation_manifest"]
        ]
        assert len(manifests) == 1
        assert manifests[0]["status"] == "已生效"
