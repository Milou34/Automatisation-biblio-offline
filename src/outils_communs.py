from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, Union
import pandas as pd
from openpyxl import load_workbook


@dataclass(frozen=True)
class LocalINPNPaths:
    parquet_dir: Path
    znieff_espece: Path
    znieff_habitats: Path
    znieff_habitats_info: Path
    znieff_infos_generales: Path
    taxref: Path
    n2000_habitats: Path
    n2000_especes_inscrites: Path
    n2000_especes_autres: Path
    n2000_infos_generales: Path
    habref_70: Path

    @staticmethod
    def default(parquet_dir: Path | str = "data/parquet") -> "LocalINPNPaths":
        pdir = Path(parquet_dir)
        return LocalINPNPaths(
            parquet_dir=pdir,
            znieff_espece=pdir / "ZNIEFF_Especes.parquet",
            znieff_habitats=pdir / "ZNIEFF_Habitats.parquet",
            znieff_habitats_info=pdir / "ZNIEFF_Habitats_infos.parquet",
            znieff_infos_generales=pdir / "ZNIEFF_Infos_generales.parquet",
            taxref=pdir / "TAXREFv18.parquet",
            n2000_habitats=pdir / "N2000_Habitats.parquet",
            n2000_especes_inscrites=pdir / "N2000_Especes_inscrites.parquet",
            n2000_especes_autres=pdir / "N2000_Especes_autres.parquet",
            n2000_infos_generales=pdir / "N2000_Infos_generales.parquet",
            habref_70=pdir / "HABREF_70.parquet",
        )


def ensure_exists(path: Path) -> None:
    """Vérifie qu'un fichier existe, lève une exception sinon."""
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")


def filter_parquet(
    parquet_file: Path,
    key_col: str,
    keep_cols: Sequence[str],
    codes: Sequence[str],
) -> pd.DataFrame:
    """
    Lit uniquement keep_cols depuis le parquet, puis filtre key_col ∈ codes.
    """
    ensure_exists(parquet_file)

    df = pd.read_parquet(parquet_file, columns=list(keep_cols))

    # Robustesse: on filtre en string (parfois parquet peut typer différemment)
    df[key_col] = df[key_col].astype(str)
    code_set = set(str(c) for c in codes)

    df = df[df[key_col].isin(code_set)]
    return df


def convert_csv_vers_parquet(
    raw_dir: Union[str, Path] = r"C:\Users\MarylouBERTIN\Documents\Automatisation biblio\Automatisation biblio - CSV\data\raw_csv",
    parquet_dir: Union[str, Path] = r"C:\Users\MarylouBERTIN\Documents\Automatisation biblio\Automatisation biblio - CSV\data\parquet",
    sep: str = ";",
    encoding: str = "utf-8",
    engine: str = "pyarrow",
    compression: str = "snappy",
    force: bool = False,
):
    """Convertit tous les CSV de `raw_dir` vers des fichiers parquet dans `parquet_dir`.

    Si un fichier parquet existe déjà (même nom stem), il est ignoré sauf si `force=True`.
    """

    RAW_DIR = Path(raw_dir)
    PARQUET_DIR = Path(parquet_dir)

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    for csv_file in RAW_DIR.glob("*.csv"):
        parquet_path = PARQUET_DIR / (csv_file.stem + ".parquet")

        if parquet_path.exists() and not force:
            print(f"Ignoré (existe): {csv_file.name} → {parquet_path.name}")
            continue

        print(f"Conversion de {csv_file.name}...")

        try:
            df = pd.read_csv(
                csv_file,
                sep=sep,
                encoding=encoding,
                dtype=str,
                low_memory=False,
            )

            df.to_parquet(
                parquet_path,
                index=False,
                engine=engine,
                compression=compression,
            )

            print(f"✔ → {parquet_path.name}")

        except Exception as e:
            print(f"❌ Erreur sur {csv_file.name}: {e}")

    print("\nConversion terminée.")


def write_excel_output(
    out_xlsx: Path,
    df_habitats_znieff: pd.DataFrame,
    df_especes_znieff: pd.DataFrame,
    df_habitats_n2000: pd.DataFrame = None,
    df_especes_n2000: pd.DataFrame = None,
    sheet_habitats_znieff: str = "Habitats ZNIEFF",
    sheet_especes_znieff: str = "Espèces ZNIEFF",
    sheet_habitats_n2000: str = "Habitats N2000",
    sheet_especes_n2000: str = "Espèces N2000",
) -> Path:
    """Écrit les dataframes dans un fichier Excel avec plusieurs onglets et ajuste la largeur des colonnes."""
    
    # Convertir les None en DataFrames vides
    if df_habitats_n2000 is None:
        df_habitats_n2000 = pd.DataFrame()
    if df_especes_n2000 is None:
        df_especes_n2000 = pd.DataFrame()
    
    # Vérifier si aucune donnée n'a été trouvée
    if (df_habitats_znieff.empty and df_especes_znieff.empty and 
        df_habitats_n2000.empty and df_especes_n2000.empty):
        raise ValueError("Aucune donnée trouvée pour les codes saisis")
    
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df_habitats_znieff.to_excel(writer, sheet_name=sheet_habitats_znieff, index=False)
        df_especes_znieff.to_excel(writer, sheet_name=sheet_especes_znieff, index=False)
        df_habitats_n2000.to_excel(writer, sheet_name=sheet_habitats_n2000, index=False)
        df_especes_n2000.to_excel(writer, sheet_name=sheet_especes_n2000, index=False)

    # Charger le workbook avec openpyxl et ajuster les largeurs de colonnes
    wb = load_workbook(out_xlsx)
    
    for sheet in wb.sheetnames:
        ws = wb[sheet]
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            
            for cell in column:
                try:
                    if cell.value:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                except:
                    pass
            
            # Ajouter un peu de padding et appliquer une largeur min/max raisonnable
            adjusted_width = min(50, max(12, max_length + 2))
            ws.column_dimensions[column_letter].width = adjusted_width
    
    wb.save(out_xlsx)
    return out_xlsx
