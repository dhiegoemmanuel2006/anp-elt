import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_FILES = [
    BASE_DIR / "data" / "precos2semestre.csv",
    BASE_DIR / "data" / "precos1semestre.csv"
]

OUTPUT_PATH = BASE_DIR / "data" / "output" / "precos_combustivel_bruto.parquet"


def extract_data(file_path):
    """
    Extrai os dados de um arquivo CSV e retorna um DataFrame do Pandas.

    Args:
        file_path (str): O caminho para o arquivo CSV.

    Returns:
        pd.DataFrame: Um DataFrame contendo os dados extraídos do arquivo CSV.
    """
    try:
        data = pd.read_csv(file_path, sep=";", dtype=str, low_memory=False)
        return data
    except Exception as e:
        print(f"Erro ao extrair dados do arquivo {file_path}: {e}")
        return None

def load_data_in_output_path(data, output_path):
    """
    Salva o DataFrame em um arquivo Parquet no caminho especificado.

    Args:
        data (pd.DataFrame): O DataFrame a ser salvo.
        output_path (str): O caminho para o arquivo Parquet de saída.
    """
    try:
        data.to_parquet(output_path, index=False)
        print(f"Dados salvos com sucesso em {output_path}")
    except Exception as e:
        print(f"Erro ao salvar dados em {output_path}: {e}")

if __name__ == "__main__":
    """
    Função principal que executa o processo de extração e carregamento dos dados.
    """
    # Garantindo que o diretório de saída exista
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    combined_data = pd.DataFrame()

    for file in INPUT_FILES:
        data = extract_data(file)
        if data is not None:
            combined_data = pd.concat([combined_data, data], ignore_index=True)

    if not combined_data.empty:
        load_data_in_output_path(combined_data, OUTPUT_PATH)
    else:
        print("Nenhum dado foi extraído para salvar.")