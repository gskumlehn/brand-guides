from flask_cors import CORS

_ALLOWED_ORIGINS = [
    r"https://.*\.lovableproject\.com",
    r"https://.*\.lovable\.app",
    r"https://.*\.run\.app",           # Cloud Run pré-visualizações
    r"http://localhost:\d+",           # dev
    r"http://127\.0\.0\.1:\d+",
    "*"
]

CORS(
    app,
    origins=_ALLOWED_ORIGINS,
    supports_credentials=False,  # não usamos cookies
    allow_headers="*",           # reflete os headers pedidos no preflight
    expose_headers=["Content-Length","Content-Type"],
    methods=["GET", "POST", "OPTIONS"]
)
