import pandas as pd
import json

print('Iniciando o ETL')
# lendo o csv do dataset escolhido
df = pd.read_csv("raw/Brazil_Clothing_E-commerce_Dataset.csv", sep=",")

print('DataFrame inicial:', df.shape)

# renomeando as colunas para snake_case
df.columns = df.columns.str.lower()
# manualmente substituindo os caracteres especiais
df.columns = (
    df.columns
    .str.replace('í', 'i')
    .str.replace('ç','c')
    .str.replace('ã', 'a')
    .str.replace('õ', 'o')
    .str.replace('ê', 'e'))

#criando um novo dataframe para os dados limpos
df_limpo = pd.DataFrame({
    'id':df['id'],
    'titulo':df['titulo'],
})

# removendo os nulos 
df_limpo['nota'] = df['nota'].fillna(0)
df_limpo['n_avaliacoes'] = df['n_avaliacoes'].fillna('0')
df_limpo['marca'] = df['marca'].fillna('N/A')
df_limpo['material'] = df['material'].fillna('N/A')
df_limpo['genero'] = df['genero'].fillna("N/A")
df_limpo['temporada'] = df['temporada'].fillna('N/A')
df_limpo['review1'] = df['review1'].fillna('N/A')
df_limpo['review2'] = df['review2'].fillna('N/A')
df_limpo['review3'] = df['review3'].fillna('N/A')

# removendo os parênteses da coluna de número de avaliações
df_limpo['n_avaliacoes'] = (
df_limpo['n_avaliacoes']
    .str.replace('(', '',)
    .str.replace(')', '',)
    .astype(int)
)

# somando os reais e centavos para a coluna de preço
df_limpo['preco_com_desconto'] = ((df['centavos'].fillna(0)/100)+df['reais']).round(2)

# calculando o desconto percentual baseado na coluna desconto
df_limpo['desconto_percentual'] = (
    df['desconto']
    .fillna('0')
    .str.replace('% OFF', '',)
    .astype(float) / 100
).round(2)

# calculando o preço original
df_limpo['preco_original'] = (df_limpo['preco_com_desconto'] / (1 - df_limpo['desconto_percentual'])).round(2)

# criando a coluna de 'tem desconto' e uma função para preenche-la
def tem_desconto(desconto):
    if desconto == 0.00:
        return 'Não'
    return 'Sim'

df_limpo['tem_desconto'] = df_limpo['desconto_percentual'].apply(tem_desconto)

print('DataFrame limpo:', df_limpo.shape)

# verificações de qualidade

# contagem de linhas
total_linhas = len(df_limpo)
# contagem de nulos
total_nulos = (df_limpo.isnull().sum()).to_dict()
# possíveis valores da coluna tem desconto
valores_tem_desconto = df_limpo['tem_desconto'].unique().tolist()
# total de linhas duplicadas
total_duplicadas = int(df_limpo.duplicated().sum())

# criando um dicionário
relatorio_qualidade = {
    'total_linhas': total_linhas,
    'total_nulos': total_nulos,
    'valores_tem_desconto': valores_tem_desconto,
    'total_duplicadas': total_duplicadas
}

#log do dicionário no terminal
print('Relatório de Qualidade:',relatorio_qualidade)

# salvando em json no data_quality_report
with open('processed/data_quality_report.json', "w", encoding="utf-8") as f:
    json.dump(relatorio_qualidade, f, ensure_ascii=False, indent=4)

# métricas
# encontrando os produtos com top 5 maiores descontos
top5_descontos = df_limpo.sort_values('desconto_percentual', ascending=False).head(5)['titulo'].tolist()
# distribuição de valores na coluna temporada
distribuicao_temporada = df_limpo['temporada'].value_counts().to_dict()
# média de avaliações
media_avaliacoes = (df_limpo['n_avaliacoes'].mean()).round(2)
# média de preços com os descontos aplicados
media_preco_com_desconto = (df_limpo['preco_com_desconto'].mean()).round(2)

# criando dicionário
metricas = {
    'top5_descontos': top5_descontos,
    'distribuicao_por_temporada': distribuicao_temporada,
    'media_n_avaliacoes': media_avaliacoes,
    'media_preco_com_desconto': media_preco_com_desconto
}

# salvando o dicionário em json
with open('processed/metrics.json', "w", encoding="utf-8") as f:
    json.dump(metricas, f, ensure_ascii=False, indent=4)

# salvando o dataframe limpo em parquet
df_limpo.to_parquet('Warehouse/Brazil_Clothing_E-commerce_Limpo202511part-1.parquet', engine="pyarrow", index=False)

print('ETL finalizado!')