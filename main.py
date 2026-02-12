"""Point d'entrée du script d'export biodiversité (ZNIEFF / Natura 2000)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re

from src.n2000 import export_especes_n2000, export_habitats_n2000, parse_codes_n2000
from src.outils_communs import LocalINPNPaths, write_excel_output
from src.znieff import export_especes_znieff, export_habitats_znieff, parse_codes_znieff


def ask_codes() -> tuple[list[str], list[str]]:
    """Demande et valide les codes ZNIEFF/N2000 jusqu'à obtenir au moins un code."""
    while True:
        while True:
            raw_znieff = input("Codes ZNIEFF séparés par ; ou , :\n")
            try:
                codes_znieff = parse_codes_znieff(raw_znieff)
                break
            except ValueError as error:
                print(f"Erreur: {error}")

        while True:
            raw_n2000 = input("Codes Natura 2000 séparés par ; ou , :\n")
            try:
                codes_n2000 = parse_codes_n2000(raw_n2000)
                break
            except ValueError as error:
                print(f"Erreur: {error}")

        if codes_znieff or codes_n2000:
            return codes_znieff, codes_n2000

        print("Aucun code ZNIEFF ou N2000 fourni. Merci de saisir au moins un code.")


def ask_project_name() -> str:
    """Demande un nom de projet et le normalise pour un nom de fichier Windows."""
    project_name = input("Nom du projet :\n").strip()
    if not project_name:
        project_name = "SansNom"
    return re.sub(r'[\\/:*?"<>|]+', "_", project_name)


def ask_output_directory() -> Path:
    """Demande un dossier de sortie valide (obligatoire) et le crée si nécessaire."""
    while True:
        output_dir_input = input("Chemin où créer l'Excel final :\n").strip().strip('"')
        if not output_dir_input:
            print("Le chemin est obligatoire.")
            continue

        candidate = Path(output_dir_input)
        try:
            if candidate.exists() and not candidate.is_dir():
                print(f"Chemin invalide: {candidate} existe mais ce n'est pas un dossier.")
                continue
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except (OSError, ValueError) as error:
            print(f"Chemin invalide: {error}")


def ask_continue() -> bool:
    """Demande si l'utilisateur souhaite générer une autre bibliographie."""
    while True:
        answer = input("\nContinuer avec une autre bibliographie ? \nAppuyez sur O pour Oui ou N pour Non :\n").strip().upper()
        if answer == "O":
            return True
        if answer == "N":
            return False
        print("Réponse invalide. Appuyez sur O pour Oui ou N pour Non.")


def run_single_export() -> None:
    """Exécute un flux complet de lecture, filtrage et export Excel."""
    codes_znieff, codes_n2000 = ask_codes()
    project_name = ask_project_name()
    output_dir = ask_output_directory()

    base_dir = Path(__file__).resolve().parent
    paths = LocalINPNPaths.default(base_dir / "data")

    print("Lecture / filtrage habitats ZNIEFF...")
    df_habitats_znieff = export_habitats_znieff(paths, codes_znieff)

    print("Lecture / filtrage espèces ZNIEFF...")
    df_especes_znieff = export_especes_znieff(paths, codes_znieff)

    print("Lecture / filtrage habitats Natura 2000...")
    df_habitats_n2000 = export_habitats_n2000(paths, codes_n2000)

    print("Lecture / filtrage espèces Natura 2000...")
    df_especes_n2000 = export_especes_n2000(paths, codes_n2000)

    stamp = datetime.now().strftime("%d%m%Y")
    out_xlsx = output_dir / f"Bibliographie_{project_name}_{stamp}.xlsx"

    try:
        write_excel_output(
            out_xlsx,
            df_habitats_znieff,
            df_especes_znieff,
            df_habitats_n2000,
            df_especes_n2000,
        )
    except ValueError as error:
        print(f"\n❌ {error}")
        return
    except OSError as error:
        print(
            "\n❌ Erreur lors de la création de l'Excel "
            f"({type(error).__name__}) : {error}"
        )
        return

    print(f"\n✅ Excel généré : {out_xlsx}")
    print(f"   - HABITATS ZNIEFF : {len(df_habitats_znieff)} lignes")
    print(f"   - ESPECES ZNIEFF : {len(df_especes_znieff)} lignes")
    print(f"   - HABITATS N2000 : {len(df_habitats_n2000)} lignes")
    print(f"   - ESPECES N2000 : {len(df_especes_n2000)} lignes")


def main() -> None:
    """Lance le programme et propose d'enchaîner plusieurs bibliographies."""
    is_first_run = True
    while True:
        if not is_first_run:
            print("\n----- Nouvelle bibliographie -----\n")
        run_single_export()
        if not ask_continue():
            print("Fin du programme.")
            break
        is_first_run = False

if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # pylint: disable=broad-exception-caught
        print(
            "\n❌ Erreur inattendue "
            f"({type(error).__name__}) : {error}"
        )
