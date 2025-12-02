import sqlite3
import pandas as pd

# valodação das métricas por SQLite

# lendo o parquet usando pandas
df = pd.read_parquet('warehouse/Brazil_Clothing_E-commerce_Limpo202511part-1.parquet')

# criando o database
con = sqlite3.connect('Warehouse.db')
cursor = con.cursor()

# criando a tabela usando o dataframe
df.to_sql('produto_reviw', con, if_exists='replace', index=False)

# encontrando os produtos com top 5 maiores descontos
cursor.execute(
    'SELECT titulo, desconto_percentual FROM produto_reviw ORDER BY desconto_percentual DESC LIMIT 5'
)
rows = cursor.fetchall()
for r in rows:
    print(r)

# distribuição de valores na coluna temporada
cursor.execute(
               'SELECT temporada, COUNT(*) AS total FROM produto_reviw GROUP BY temporada ORDER BY total DESC')
rows = cursor.fetchall()
for r in rows:
    print(r)

# média de avaliações e de preços com desconto
cursor.execute(
    "SELECT AVG(n_avaliacoes) as media_avaliacoes, AVG(preco_com_desconto) as media_precos FROM produto_reviw"
)
rows = cursor.fetchall()
for r in rows:
    print(r)
