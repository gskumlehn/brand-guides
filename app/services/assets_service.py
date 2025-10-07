# app/services/assets_service.py
from typing import List, Dict, Any
from ..repositories.assets_repository import AssetsRepository
from ..repositories.storage_repository import build_prefix

class AssetsService:
    def __init__(self) -> None:
        self.repo = AssetsRepository()

    def sidebar(self, brand: str) -> List[Dict[str, Any]]:
        rows = self.repo.get_nav_structure(brand)
        out: List[Dict[str, Any]] = []
        for r in rows:
            subs = []
            for s in r.get("subcategories", []):
                subs.append({
                    "subcategory_seq": int(s["subcategory_seq"]) if s["subcategory_seq"] is not None else None,
                    "subcategory_key": s.get("subcategory_key") or "",
                    "subcategory_label": s.get("subcategory_label") or "",
                    "columns": int(s["columns"]) if s.get("columns") is not None else None,
                })
            out.append({
                "category_seq": int(r["category_seq"]),
                "category_key": r.get("category_key") or "",
                "category_label": r.get("category_label") or "",
                "subcategories": subs
            })
        return out

    def gallery(self, brand: str) -> List[Dict[str, Any]]:
        rows = self.repo.get_gallery_rows(brand)
        by_cat: Dict[str, Any] = {}
        for r in rows:
            ckey = r["category_key"]; skey = r["subcategory_key"] or ""
            c = by_cat.setdefault(ckey, {
                "category_seq": int(r["category_seq"]),
                "category_key": ckey,
                "category_label": r["category_label"],
                "subcategories": {}
            })
            s = c["subcategories"].setdefault(skey, {
                "subcategory_seq": int(r["subcategory_seq"]) if r["subcategory_seq"] is not None else None,
                "subcategory_key": skey,
                "subcategory_label": r["subcategory_label"] or "",
                "columns": int(r["columns"]) if r["columns"] is not None else None,
                "stream_url": f"/assets/stream?brand_name={brand}&category_key={ckey}" + (f"&subcategory_key={skey}" if skey else ""),
                "storage_prefix": build_prefix(brand, ckey, skey if skey else None),
                "images": []
            })
            s["images"].append({
                "sequence": int(r["sequence"]) if r["sequence"] is not None else 0,
                "original_name": r["original_name"],
                "path": r["path"],
                "url": r["url"],
                "is_original": bool(r["is_original"])
            })
        result: List[Dict[str, Any]] = []
        for c in sorted(by_cat.values(), key=lambda x: x["category_seq"]):
            subs = list(c["subcategories"].values())
            subs.sort(key=lambda x: (x["subcategory_seq"] is None, x["subcategory_seq"] or 0))
            c["subcategories"] = subs
            result.append(c)
        return result
