# app/repositories/assets_repository.py
from typing import Any, Dict, List, Optional
from ..infra.db.bq_client import q, fq


class AssetsRepository:
    def sidebar(self, brand: str) -> List[Dict[str, Any]]:
        cats_sql = f"""
        SELECT
          category_key,
          ANY_VALUE(category_label) AS category_label,
          ANY_VALUE(category_seq)   AS category_seq
        FROM {fq('assets')}
        WHERE brand_name = @brand
        GROUP BY category_key
        ORDER BY category_seq, category_key
        """
        cats = q(cats_sql, {"brand": brand})

        subs_sql = f"""
        SELECT
          category_key,
          subcategory_key,
          ANY_VALUE(subcategory_label) AS subcategory_label,
          ANY_VALUE(subcategory_seq)   AS subcategory_seq,
          ANY_VALUE(columns)           AS columns
        FROM {fq('assets')}
        WHERE brand_name = @brand
          AND subcategory_key IS NOT NULL
        GROUP BY category_key, subcategory_key
        ORDER BY category_key, subcategory_seq, subcategory_key
        """
        subs = q(subs_sql, {"brand": brand})

        subs_by_cat: Dict[str, List[Dict[str, Any]]] = {}
        for s in subs:
            subs_by_cat.setdefault(s["category_key"], []).append({
                "subcategory_key": s["subcategory_key"],
                "subcategory_label": s["subcategory_label"],
                "subcategory_seq": s["subcategory_seq"],
                "columns": s["columns"],
            })

        out: List[Dict[str, Any]] = []
        for c in cats:
            sub_list = subs_by_cat.get(c["category_key"], [])
            out.append({
                "category_key": c["category_key"],
                "category_label": c["category_label"],
                "category_seq": c["category_seq"],
                "subcategory_count": len(sub_list),
                "subcategories": sub_list,
            })
        return out

    def gallery(
        self,
        brand: str,
        category_key: Optional[str] = None,
        subcategory_seq: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        base_where = ["brand_name = @brand"]
        params: Dict[str, Any] = {"brand": brand}
        if category_key:
            base_where.append("category_key = @cat")
            params["cat"] = category_key
        if subcategory_seq is not None:
            base_where.append("subcategory_seq = @sseq")
            params["sseq"] = int(subcategory_seq)

        subs_sql = f"""
        SELECT
          category_key,
          ANY_VALUE(category_label)    AS category_label,
          ANY_VALUE(category_seq)      AS category_seq,
          subcategory_key,
          ANY_VALUE(subcategory_label) AS subcategory_label,
          ANY_VALUE(subcategory_seq)   AS subcategory_seq,
          ANY_VALUE(columns)           AS columns
        FROM {fq('assets')}
        WHERE {" AND ".join(base_where)}
        GROUP BY category_key, subcategory_key
        ORDER BY category_seq, category_key, subcategory_seq, subcategory_key
        """
        subs = q(subs_sql, params)

        cat_txt_sql = f"""
        SELECT
          category_key,
          STRING_AGG(text_content, '\\n\\n' ORDER BY sequence) AS category_text
        FROM {fq('assets')}
        WHERE {" AND ".join([w for w in base_where if not w.startswith("subcategory_seq")])}
          AND asset_type = 'text'
          AND (subcategory_key IS NULL OR subcategory_key = '')
        GROUP BY category_key
        """
        cat_txt = q(cat_txt_sql, {k: v for k, v in params.items() if k != "sseq"})
        cat_text_map = {r["category_key"]: (r["category_text"] or "").strip() for r in cat_txt}

        sub_txt_sql = f"""
        SELECT
          category_key,
          subcategory_key,
          STRING_AGG(text_content, '\\n\\n' ORDER BY sequence) AS subcategory_text
        FROM {fq('assets')}
        WHERE {" AND ".join(base_where)}
          AND asset_type = 'text'
          AND subcategory_key IS NOT NULL
        GROUP BY category_key, subcategory_key
        """
        sub_txt = q(sub_txt_sql, params)
        sub_text_map: Dict[str, Dict[str, str]] = {}
        for r in sub_txt:
            sub_text_map.setdefault(r["category_key"], {})[r["subcategory_key"]] = (r["subcategory_text"] or "").strip()

        out_by_cat: Dict[str, Dict[str, Any]] = {}
        for s in subs:
            cat_key = s["category_key"]
            cat_payload = out_by_cat.setdefault(cat_key, {
                "category_key": cat_key,
                "category_label": s["category_label"],
                "category_seq": s["category_seq"],
                "category_text": cat_text_map.get(cat_key) or "",
                "subcategories": []
            })

            img_where = [
                "brand_name = @brand",
                "category_key = @cat",
                "asset_type = 'image'"
            ]
            img_params = {"brand": brand, "cat": cat_key}

            if s["subcategory_key"] in (None, ""):
                img_where.append("(subcategory_key IS NULL OR subcategory_key = '')")
                storage_prefix = f"{brand.lower()}/{cat_key}/"
            else:
                img_where.append("subcategory_key = @subk")
                img_params["subk"] = s["subcategory_key"]
                storage_prefix = f"{brand.lower()}/{cat_key}/{s['subcategory_key']}/"

            imgs_sql = f"""
            SELECT is_original, original_name, path, url, sequence
            FROM {fq('assets')}
            WHERE {" AND ".join(img_where)}
            ORDER BY sequence, original_name
            """
            imgs = q(imgs_sql, img_params)

            cat_payload["subcategories"].append({
                "subcategory_key": s["subcategory_key"],
                "subcategory_label": s["subcategory_label"],
                "subcategory_seq": s["subcategory_seq"],
                "columns": s["columns"],
                "subcategory_text": sub_text_map.get(cat_key, {}).get(s["subcategory_key"] or "", ""),
                "storage_prefix": storage_prefix,
                # 'stream' será preenchido no service com URLs assinadas
                "images": [
                    {
                        "is_original": r["is_original"],
                        "original_name": r["original_name"],
                        "path": r["path"],
                        "url": r["url"],           # URL pública (ignorar se bucket é privado)
                        "sequence": r["sequence"],
                    } for r in imgs
                ],
            })

        out: List[Dict[str, Any]] = []
        for payload in out_by_cat.values():
            payload["subcategories"].sort(
                key=lambda x: (
                    x["subcategory_seq"] if x["subcategory_seq"] is not None else 9999,
                    x["subcategory_key"] or ""
                )
            )
            out.append(payload)
        out.sort(key=lambda c: (c["category_seq"], c["category_key"]))
        return out
