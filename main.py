from __future__ import annotations

from pathlib import Path
from datetime import datetime
import re

from src.outils_communs import (
    LocalINPNPaths,
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

#Input ZNIEFF

    while True:
        raw_znieff = input("Codes ZNIEFF séparés par ; ou , :\n")
        try:
            codes = parse_codes_znieff(raw_znieff)
            break
        except ValueError as e:
            print(f"Erreur: {e}")
            
#Input N2000

    while True:
        raw_n2000 = input("Codes Natura 2000 séparés par ; ou , :\n")
        try:
            codes_n2000 = parse_codes_n2000(raw_n2000)
            break
        except ValueError as e:
            print(f"Erreur: {e}")
            
    if not codes and not codes_n2000:
        print("Aucun code ZNIEFF ou N2000 fourni, arrêt.")
        return        
    
#Input nom du projet

    nom_projet = input("Nom du projet :\n").strip()
    if not nom_projet:
        nom_projet = "SansNom"
    nom_projet = re.sub(r'[\\/:*?"<>|]+', "_", nom_projet)

#Input dossier de sortie et vérification de validité

    out_dir = None
    while out_dir is None:
        out_dir_input = input("Chemin où créer l'Excel final :\n").strip().strip('"')
        if not out_dir_input:
            print("Le chemin est obligatoire.")
            continue

        candidate = Path(out_dir_input)
        try:
            if candidate.exists() and not candidate.is_dir():
                print(f"Chemin invalide: {candidate} existe mais ce n'est pas un dossier.")
                continue
            candidate.mkdir(parents=True, exist_ok=True)
            out_dir = candidate
        except (OSError, ValueError) as e:
            print(f"Chemin invalide: {e}")
    
#Lecture des données parquet, filtrage avec les codes saisis

    BASE_DIR = Path(__file__).resolve().parent
    paths = LocalINPNPaths.default(BASE_DIR / "data")

    print("Lecture / filtrage habitats ZNIEFF...")
    df_habitats_znieff = export_habitats_znieff(paths, codes)

    print("Lecture / filtrage espèces ZNIEFF...")
    df_especes_znieff = export_especes_znieff(paths, codes)

    print("Lecture / filtrage habitats Natura 2000...")
    df_habitats_n2000 = export_habitats_n2000(paths, codes_n2000)

    print("Lecture / filtrage espèces Natura 2000...")
    df_especes_n2000 = export_especes_n2000(paths, codes_n2000)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = out_dir / f"Bibliographie_{nom_projet}_{stamp}.xlsx"

#Écriture du fichier Excel avec les données filtrées, gestion des erreurs et affichage du résultat

    try:
        write_excel_output(out_xlsx, df_habitats_znieff, df_especes_znieff, df_habitats_n2000, df_especes_n2000)
    except ValueError as e:
        print(f"\n❌ {e}")
        return
    except Exception as e:
        print(f"\n❌ Erreur lors de la création de l'Excel ({type(e).__name__}) : {e}")
        return

    print(f"\n✅ Excel généré : {out_xlsx}")
    print(f"   - HABITATS ZNIEFF : {len(df_habitats_znieff)} lignes")
    print(f"   - ESPECES ZNIEFF : {len(df_especes_znieff)} lignes")
    print(f"   - HABITATS N2000 : {len(df_habitats_n2000)} lignes")
    print(f"   - ESPECES N2000 : {len(df_especes_n2000)} lignes")

#Lancement du script

if __name__ == "__main__":
    main()
