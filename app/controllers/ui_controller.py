import io, logging, zipfile
from flask import Blueprint, render_template, send_file

log = logging.getLogger(__name__)
ui_bp = Blueprint("ui", __name__)

@ui_bp.get("/")
def index():
    log.info("Render ingestion UI")

    return render_template("ingestion.html")

@ui_bp.get("/templates/structure.zip")
def download_structure():
    log.info("Generating structure template ZIP (.txt READMEs)")
    buf = io.BytesIO()

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        folders = ["logos/primary/","logos/secondary_vertical/","logos/secondary_horizontal/","logos/guidelines/","graphics/","avatars/","icons/","fonts/","colors/","applications/"]
        for d in folders: z.writestr(d, "")
        readmes = {
            "logos/README.txt": "logos/\n- Subpastas FIXAS: primary, secondary_vertical, secondary_horizontal, guidelines.\n- Numeração sequencial por subpasta: 01_, 02_, ...\nEx.: primary/01_master.svg\n",
            "graphics/README.txt": "graphics/\n- Subpastas DINÂMICAS = variação (persistida em subcategory).\n- Arquivos = variações de cor/estilo.\nEx.: pattern-a/01_red.svg\n",
            "avatars/README.txt": "avatars/\n- Subpastas DINÂMICAS = nome do conjunto (subcategory).\n- Arquivos = cores/estilos.\nEx.: photographic/01_fullcolor.png\n",
            "icons/README.txt": "icons/\nPasta PLANA. 01_, 02_, ...\n",
            "fonts/README.txt": "fonts/\nPasta PLANA (.woff2/.ttf). 01_, 02_, ...\n",
            "colors/README.txt": "colors/\nPasta PLANA. 01_, 02_, ...\n",
            "applications/README.txt": "applications/\nPasta PLANA. 01_, 02_, ...\n",
        }
        for path, content in readmes.items(): z.writestr(path, content)

    buf.seek(0)

    return send_file(buf, mimetype="application/zip", as_attachment=True, download_name="brand-guides-estrutura.zip")
