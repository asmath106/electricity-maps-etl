import os
import pandas as pd
from utils import get_base_paths


def load_data(path):
    return pd.read_parquet(os.path.join(path, "data.parquet"))


def build_energy_mix(df):
    df = df.copy()
    total = df["powerProductionTotal"].replace(0, 1)

    df["solar_pct"] = df["production_solar"] / total
    df["wind_pct"] = df["production_wind"] / total
    df["hydro_pct"] = df["production_hydro"] / total
    df["nuclear_pct"] = df["production_nuclear"] / total
    df["coal_pct"] = df["production_coal"] / total
    df["gas_pct"] = df["production_gas"] / total

    return df[[
        "zone", "data_datetime",
        "solar_pct", "wind_pct", "hydro_pct",
        "nuclear_pct", "coal_pct", "gas_pct"
    ]]


def build_import_export(df):
    df = df.copy()
    df["net_import_export"] = df["powerImportTotal"] - df["powerExportTotal"]

    return df[[
        "zone", "data_datetime",
        "powerImportTotal", "powerExportTotal",
        "net_import_export"
    ]]


def write_data(df, path):
    os.makedirs(path, exist_ok=True)
    df.to_parquet(os.path.join(path, "data.parquet"), index=False)


def run_gold():
    paths = get_base_paths()

    flows_path = os.path.join(paths["silver"], "electricity_flows", "parquet")

    df = load_data(flows_path)

    energy_mix = build_energy_mix(df)
    imports = build_import_export(df)

    write_data(energy_mix, os.path.join(paths["gold"], "energy_mix"))
    write_data(imports, os.path.join(paths["gold"], "import_export"))


if __name__ == "__main__":
    run_gold()