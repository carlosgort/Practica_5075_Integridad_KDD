import hashlib
import logging
import shutil
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "raw_batch_202611"
MANIFEST_PATH = RAW_DIR / "manifest_checksums.sha256"
STAGING_VERIFIED_DIR = BASE_DIR / "staging" / "verified"
QUARANTINE_TAMPERED_DIR = BASE_DIR / "quarantine" / "tampered"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "ejecucion_pipeline.log"


def configurar_logger() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("PipelineKDD.Fase1_Crypto")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formato = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | [FASE 1] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    fh.setFormatter(formato)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formato)
    logger.addHandler(ch)

    return logger


def calcular_sha256(ruta_fichero: Path) -> str:
    hasher = hashlib.sha256()
    with ruta_fichero.open("rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def validar_lote() -> dict:
    logger = configurar_logger()

    STAGING_VERIFIED_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_TAMPERED_DIR.mkdir(parents=True, exist_ok=True)

    if not MANIFEST_PATH.exists():
        logger.critical(f"Manifiesto de control no encontrado en: {MANIFEST_PATH}")
        sys.exit(1)

    logger.info("=== INICIO DE VERIFICACIÓN CRIPTOGRÁFICA SHA-256 POR LOTES ===")
    logger.info(f"Manifiesto oficial: {MANIFEST_PATH.relative_to(BASE_DIR).as_posix()}")

    resumen = {"verificados": 0, "cuarentena": 0, "ausentes": 0}
    declarados = set()

    with MANIFEST_PATH.open("r", encoding="utf-8") as manifiesto:
        for num_linea, linea in enumerate(manifiesto, start=1):
            linea_limpia = linea.strip()
            if not linea_limpia or linea_limpia.startswith("#"):
                continue

            partes = linea_limpia.split(maxsplit=1)
            if len(partes) != 2:
                logger.warning(f"Línea {num_linea} malformada en manifiesto: '{linea_limpia}'")
                continue

            hash_esperado = partes[0].lower()
            nombre_fichero = Path(partes[1].lstrip("*").strip()).name
            declarados.add(nombre_fichero)

            ruta_origen = RAW_DIR / nombre_fichero

            if not ruta_origen.exists():
                logger.error(
                    f"[AUSENTE] '{nombre_fichero}' declarado en manifiesto pero no encontrado en {RAW_DIR.name}/"
                )
                resumen["ausentes"] += 1
                continue

            hash_calculado = calcular_sha256(ruta_origen).lower()

            if hash_calculado == hash_esperado:
                ruta_destino = STAGING_VERIFIED_DIR / nombre_fichero
                shutil.move(str(ruta_origen), str(ruta_destino))
                logger.info(
                    f"[OK] '{nombre_fichero}' ÍNTEGRO (SHA-256: {hash_calculado[:16]}...). "
                    f"Movido a staging/verified/"
                )
                resumen["verificados"] += 1
            else:
                ruta_destino = QUARANTINE_TAMPERED_DIR / nombre_fichero
                shutil.move(str(ruta_origen), str(ruta_destino))
                logger.error(
                    f"[ALERTA SEGURIDAD] '{nombre_fichero}' CORRUPTO O MANIPULADO. "
                    f"Esperado: {hash_esperado[:16]}... | Calculado: {hash_calculado[:16]}... "
                    f"Aislado en quarantine/tampered/"
                )
                resumen["cuarentena"] += 1

    for item in RAW_DIR.iterdir():
        if item.is_file() and item.name != MANIFEST_PATH.name and item.name not in declarados:
            logger.warning(f"[NO DECLARADO] '{item.name}' presente en {RAW_DIR.name}/ sin firma en manifiesto.")

    logger.info(
        f"=== FIN FASE 1 | Verificados: {resumen['verificados']} | "
        f"En cuarentena: {resumen['cuarentena']} | Ausentes: {resumen['ausentes']} ==="
    )
    return resumen


if __name__ == "__main__":
    validar_lote()
