from __future__ import annotations

from pathlib import Path
from datetime import datetime

from src.outils_communs import (
    LocalINPNPaths,
    convert_csv_vers_parquet,
    write_excel_output,
)
from src.znieff import (
    parse_codes_znieff,
    export_especes_znieff,
    export_habitats_znieff,
)
from src.n2000 import (
    parse_codes_n2000,
    export_habitats_n2000,
    export_especes_n2000,
)



def main():

    raw = input("Codes ZNIEFF séparés par ; ou , :\n")
    try:
        codes = parse_codes_znieff(raw)
    except ValueError as e:
        print(f"Erreur: {e}")
        return

    raw_n2000 = input("Codes Natura 2000 séparés par ; ou , :\n")
    try:
        codes_n2000 = parse_codes_n2000(raw_n2000)
    except ValueError as e:
        print(f"Erreur: {e}")
        return

    if not codes and not codes_n2000:
        print("Aucun code ZNIEFF ou N2000 fourni, arrêt.")
        return

    BASE_DIR = Path(__file__).resolve().parent
    # Vérifie si les fichiers CSV sont convertis en parquet
    convert_csv_vers_parquet(
        raw_dir=BASE_DIR / "data" / "raw_csv",
        parquet_dir=BASE_DIR / "data" / "parquet",
        force=False,
    )

    paths = LocalINPNPaths.default(BASE_DIR / "data" / "parquet")

    print("Lecture / filtrage habitats ZNIEFF...")
    df_habitats_znieff = export_habitats_znieff(paths, codes)

    print("Lecture / filtrage espèces ZNIEFF...")
    df_especes_znieff = export_especes_znieff(paths, codes)

    print("Lecture / filtrage habitats Natura 2000...")
    df_habitats_n2000 = export_habitats_n2000(paths, codes_n2000)

    print("Lecture / filtrage espèces Natura 2000...")
    df_especes_n2000 = export_especes_n2000(paths, codes_n2000)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    BASE_DIR = Path(__file__).resolve().parent
    out_xlsx = BASE_DIR / "output" / f"export_biodiv_{stamp}.xlsx"

    try:
        write_excel_output(out_xlsx, df_habitats_znieff, df_especes_znieff, df_habitats_n2000, df_especes_n2000)
    except ValueError as e:
        print(f"\n❌ {e}")
        return

    print(f"\n✅ Excel généré : {out_xlsx}")
    print(f"   - HABITATS ZNIEFF : {len(df_habitats_znieff)} lignes")
    print(f"   - ESPECES ZNIEFF : {len(df_especes_znieff)} lignes")
    print(f"   - HABITATS N2000 : {len(df_habitats_n2000)} lignes")
    print(f"   - ESPECES N2000 : {len(df_especes_n2000)} lignes")


if __name__ == "__main__":
    main()
