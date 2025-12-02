1- Para configurar o projeto é necessário instalar os componentes presentes no requirements.txt, executando:
pip install -r requirements.txt

2- Para executar o código, vamos para o diretório data e rodamos o etl.py, assim:
cd data
python etl.py

3- O dataset escolhido foi o https://www.kaggle.com/datasets/ulissescastro/brazil-clothing-marketplace-dataset?resource=download

4- Para as regras de limpeza: transformei os nulos em 0 e 'N/A', retirei textos como parênteses e '% OFF' ficando apenas com os valores numéricos e criei 3 colunas derivadas do preço dos produtos

5- As verificações de qualidade: contagem total de linhas após a limpeza, total de nulos após a limpeza, uma lista de possíveis valores da coluna tem_desconto e o totalde linhas duplicadas

6- Para o arquio de métricas escolhi representar os 5 produtos com mais desconto, a distribuição por temporada e a média da quantidade de avaliações e dos preços com desconto aplicado. EX:
   "media_n_avaliacoes": 144.15,
    "media_preco_com_desconto": 106.95

7- Utilizei o SQLite para algumas consultas validando as métricas, para executar o arquivo utilizamos:
python utils.py

7- Como limitação principal creio que ter escolhido um dataset com datas teria sido mais vantajoso, e com mais possibilidades para métricas. Como próximo passo gostaria me incrementar os logs e adicionar mais registros.

