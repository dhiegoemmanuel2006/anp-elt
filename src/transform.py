import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_PATH = BASE_DIR / "data" / "output" / "precos_combustivel_bruto.parquet"

OUTPUT_PATH = BASE_DIR / "data" / "output" / "precos_combustivel_transformado.parquet"

""" 
Pegando os dados do arquivo Parquet,
realizando as tranformações necessárias que foram 
identificadas na etapa de análise exploratória dos dados 
e salvando o resultado em um novo arquivo Parquet.
"""

NEW_NAME_OF_COLUMNS = {
    "Regiao - Sigla": "Regiao",
    "Estado - Sigla": "Estado",
    "Municipio": "Cidade",
    "Revenda": "Revendedor",
    "CNPJ da Revenda": "CNPJ do Revendedor",
    "Produto": "Tipo de Combustível",
    "Bandeira": "Fornecedor"
}

def transform_data(data):
    """
    Realiza as transformações necessárias nos dados.

    Args:
        data (pd.DataFrame): O DataFrame contendo os dados a serem transformados.
    """
    try:
        rename_columns(data)
        drop_unnecessary_columns(data)
        convert_data_types(data)
        remove_invalid_registers(data)
    except Exception as e:
        print(f"Error occurred during data transformation: {e}")


def rename_columns(data):
    """
    Renomeia as colunas do DataFrame de acordo com o dicionário de mapeamento.

    Args:
        data (pd.DataFrame): O DataFrame cujas colunas serão renomeadas.
    """
    try:
        data.rename(columns=NEW_NAME_OF_COLUMNS, inplace=True)
    except Exception as e:
        print(f"Error occurred while renaming columns: {e}")    

def drop_unnecessary_columns(data):
    """
    Remove colunas desnecessárias do DataFrame.

    Args:
        data (pd.DataFrame): O DataFrame do qual as colunas serão removidas.
    """
    try:
        data.drop('Valor de Compra', axis=1, inplace=True)
    except Exception as e:
        print(f"Error occurred while dropping column: {e}")

def convert_data_types(data):
    """
    Converte os tipos de dados das colunas para os formatos apropriados.

    Args:
        data (pd.DataFrame): O DataFrame cujas colunas terão seus tipos de dados convertidos.
    """
    try:
        data['Valor de Venda'] = data['Valor de Venda'].str.replace(',', '.').astype('float')
        data['Data da Coleta'] = pd.to_datetime(data['Data da Coleta'], format='%d/%m/%Y', errors='coerce')
    except Exception as e:
        print(f"Error occurred while transforming data: {e}")

def remove_invalid_registers(data):
    """
    Remove linhas com datas inválidas e valores nulos essenciais para a análise.

    Args:
        data (pd.DataFrame): O DataFrame do qual as linhas serão removidas.
    """
    # Removendo linhas com datas inválidas
    
    try:
        data = data[data['Data da Coleta'] != 'NaT']

    # Removendo colunas onde os valores são nulos, pois são essenciais para a análise
        data.dropna(subset=['Tipo de Combustível', 'Revendedor', 'CNPJ do Revendedor', 'Fornecedor', 'Valor de Venda', 'Data da Coleta'], inplace=True)
    except Exception as e:
            print(f"Error occurred while removing invalid registers: {e}")

if __name__ == '__main__':
    """
        Função principal responsável pela execução do processo de transformação dos dados,
    """
    df = pd.read_parquet(INPUT_PATH)
    transform_data(df)
    print("Data transformation completed successfully.")
    df.to_parquet(OUTPUT_PATH, index=False)