# ============================================================
# 文件名: materialize_canonical_asset_v0001.py
# 中文名: 规范化来源资产命名
# 版本号: v0001
# 主层级: action
# 层级: infrastructures / provenance
# 脚本定位: 为规范化来源生成稳定的发现副本
# 职责说明:
# - 原始来源 SHA 绑定资产文件名，避免不同输入共用坐标
# 本脚本做什么:
# - 校验输入哈希，逐字节复制，保存别名来源凭证
# 本脚本不做什么:
# - 不改变规范化内容，不移动原始来源，不写数据库
# 制度边界声明:
# - 已有同名但不同哈希的副本拒绝覆盖
# 可更新: True
# ============================================================
# ALIAS_META
# alias: materialize_canonical_asset_v0001
# family: materialize_canonical_asset
# role: canonical_asset_namespace
# version: v0001
# status: active
# entry_point: scripts/action/infrastructures/materialize_canonical_asset_v0001.py
# input:
#   - canonical path and original source
# output:
#   - stable discovery copy and provenance manifest
# depends_on:
#   - intermediate_retention_contract_v0001
# used_by:
#   - data_action_chain_pipeline_v0017
# ============================================================
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from intermediate_retention_contract_v0001 import RetentionError, cli_result, no_links, sha, write

DEFAULT_ENCODING = "utf-8"
SCRIPT_FAMILY = "materialize_canonical_asset"
SCRIPT_NAME = "materialize_canonical_asset_v0001"
SCRIPT_VERSION = "v0001"

# ============================================================
# 核心业务区
# ============================================================
def materialize(source: Path, canonical: Path, expected: str, dry_run=False) -> dict:
    source, canonical = no_links(source), no_links(canonical)
    source_hash = sha(source)
    if sha(canonical) != expected:
        raise RetentionError("Canonical hash differs from ingress evidence")
    target = canonical.with_name("a_" + source_hash[:20] + ".json")
    if target.exists() and sha(target) != expected:
        raise RetentionError("Asset namespace collision; refusing overwrite")
    result = {"status": "dry_run" if dry_run else "completed", "source": str(source), "source_sha256": source_hash, "canonical_path": str(canonical), "canonical_sha256": expected, "discovery_path": str(target), "asset_id": target.stem}
    if not dry_run:
        if not target.exists():
            with canonical.open("rb") as src, target.open("xb") as dst:
                shutil.copyfileobj(src, dst)
        if sha(target) != expected:
            raise RetentionError("Discovery copy failed verification")
        write(target.with_suffix(".provenance.json"), result)
    return result

# ============================================================
# CLI / main 接口区
# ============================================================
def main():
    p = argparse.ArgumentParser()
    for field in ("source", "canonical", "canonical-sha256"):
        p.add_argument("--" + field, required=True)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    return cli_result(lambda: materialize(Path(a.source), Path(a.canonical), a.canonical_sha256, a.dry_run))

if __name__ == "__main__":
    raise SystemExit(main())
