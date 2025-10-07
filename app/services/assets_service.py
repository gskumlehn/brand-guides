# app/services/assets_service.py
from typing import List, Dict, Any
from ..repositories.assets_repository import AssetsRepository

class AssetsService:
    def __init__(self):
        self.repo = AssetsRepository()

    def sidebar(self, brand: str) -> List[Dict[str, Any]]:
        rows = self.repo.list_categories_and_subs(brand)
        out: List[Dict[str, Any]] = []
        by_cat: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            ck = r["category_key"]
            if ck not in by_cat:
                by_cat[ck] = {
                    "category_key": ck,
                    "category_label": r["category_label"],
                    "category_seq": r["category_seq"] or 0,
                    "subcategories": []
                }
            # regra: não listar nó “vazio” se há subcategorias válidas
            sk = r.get("subcategory_key")
            slabel = r.get("subcategory_label")
            sseq = r.get("subcategory_seq")
            cols = r.get("columns")
            # filtra subcategoria vazia quando existem outras
            if sk in (None, "") and slabel in (None, "") and sseq in (None, 0):
                continue
            # se for NN-NN/NN--NN, key deve ser dirname (já persistimos como null na tabela);
            # aqui assumimos que a consulta retorna subcategory_key normalizada.
            by_cat[ck]["subcategories"].append({
                "subcategory_key": sk or "",
                "subcategory_label": (slabel if slabel is not None else ""),
                "subcategory_seq": sseq,
                "columns": cols
            })

        # remove categorias sem subcategorias
        for cat in by_cat.values():
            cat["subcategories"].sort(key=lambda x: (x["subcategory_seq"] if x["subcategory_seq"] is not None else 0,
                                                     x["subcategory_key"]))
            out.append(cat)
        out.sort(key=lambda x: (x["category_seq"], x["category_key"]))
        return out

    def gallery(self, brand: str) -> List[Dict[str, Any]]:
        rows = self.repo.list_gallery(brand)
        # group: cat -> sub -> list
        cats: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            ck = r["category_key"]
            if ck not in cats:
                cats[ck] = {
                    "category_key": ck,
                    "category_label": r["category_label"],
                    "category_seq": r["category_seq"] or 0,
                    "subcategories": {}
                }
            sk = r.get("subcategory_key") or ""
            sl = r.get("subcategory_label") if r.get("subcategory_label") is not None else ""
            sseq = r.get("subcategory_seq")
            cols = r.get("columns")
            # storage_prefix sempre com dirname exato quando houver sub (se a key vier vazia, usa apenas categoria/)
            if sk:
                storage_prefix = f"{brand.lower()}/{ck}/{sk}/"
            else:
                storage_prefix = f"{brand.lower()}/{ck}/"
            sub = cats[ck]["subcategories"].setdefault(sk, {
                "subcategory_key": sk,
                "subcategory_label": sl,
                "subcategory_seq": sseq,
                "columns": cols,
                "images": [],
                "storage_prefix": storage_prefix,
                "stream_url": f"/assets/stream?brand_name={brand}&category_key={ck}" + (f"&subcategory_key={sk}" if sk else "")
            })
            sub["images"].append({
                "is_original": bool(r["is_original"]),
                "original_name": r["original_name"],
                "path": r["path"],
                "url": r["url"],
                "sequence": r["sequence"] or 0
            })

        # build list ordered
        out: List[Dict[str, Any]] = []
        for ck, c in cats.items():
            subs_list = list(c["subcategories"].values())
            subs_list.sort(key=lambda x: (x["subcategory_seq"] if x["subcategory_seq"] is not None else 0,
                                          x["subcategory_key"]))
            out.append({
                "category_key": ck,
                "category_label": c["category_label"],
                "category_seq": c["category_seq"],
                "subcategories": subs_list
            })
        out.sort(key=lambda x: (x["category_seq"], x["category_key"]))
        return out
