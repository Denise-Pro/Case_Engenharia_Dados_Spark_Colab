
# Desafio Data Engineering com Spark
Introdução
A Numa empresa de empréstimos, há necessidade de construir modelos de behavior para entender melhor os nossos clientes. O time de data engineering prepara os dados para a equipe de data science. O objetivo do exercício é transformar os dados de entrada (raw data) em uma tabela de dados agregados, chamada loan_documents.

Raw data (input)
Esses arquivos contêm dados parecidos com o que extraímos do nosso sistema:

https://noverde-data-engineering-test.s3.amazonaws.com/loans_sample.csv
https://noverde-data-engineering-test.s3.amazonaws.com/installments_sample.json
https://noverde-data-engineering-test.s3.amazonaws.com/payments_sample.parquet
Aggregated data (output)
O objetivo é gerar a tabela loan_documents, no formato parquet. Cada entry (row) da loan_documents deve conter os dados relativos a um emprestimo (loan), saber as parcelas (installments), pagamentos (payments), e algumas métricas pre-calculadas. As informações estão armazenadas usando tipos complexos (array, dictionary, nested structures). O arquivo parquet deverá ser compatível com o schema seguinte, para futuras consultas com o Hive Metastore.

Importante! Dentro do escopo deste teste, não há necessidade de verificar se de fato este schema do Hive é compatível. Esta definição serve de exemplo, para deixar mais claro qual é nosso objetivo.

CREATE EXTERNAL TABLE loan_documents (
  loan_id INT,
  period INT,
  accepted_at TIMESTAMP,
  payday INT,
  interest_rate DOUBLE,
  installments MAP<INT, STRING>,
  payments ARRAY<STRUCT<id: INT, payment_date: STRING, method: STRING, amount: DOUBLE>>,
  metrics STRUCT<latency: ARRAY<BOOLEAN>, over30: ARRAY<BOOLEAN>>
)
ROW FORMAT SERDE                                                   
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'    
WITH SERDEPROPERTIES (                                             
  'path'='<PAQUET FILE PATH>')    
STORED AS INPUTFORMAT                                              
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'  
OUTPUTFORMAT                                                       
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '<PAQUET FILE PATH>'
NOTA 1: "installments" deve ser um dicionário de (installment_number => due_date)
NOTE 2: '<PAQUET FILE PATH>' é o caminho do arquivo parquet

Linha de exemplo (representada como um doc yaml):

loan_id: 9243
period: 3
accepted_at: "2017-05-19 10:09:47.285105"
payday: 4
interest_rate: 3.12
installments:
  1: "2017-06-04"
  2: "2017-07-04"
  3: "2017-08-04"
payments:
- id: "7abaf860-9632-40e9-bfe0-13b67ded8c6f"
  payment_date: "2017-06-06"
  method: boleto
  amount: 283.09
- id: "6abaf860-9632-40e9-bfe0-33b67ded8c6d"
  payment_date: "2017-07-01"
  method: boleto
  amount: 280.00
metrics:
  latency: [false, false, false, true, true, true, false, ...]
  over30: [false, false, false, false, false, false, false, ...]
Obs: esses são valores fake

Métricas (metrics)
Cada métrica está armazenada na forma de um array, cujo index é o número de dias desde a originação (loan.accepted_at)

metric[0] é o valor da métrica no dia da originação
metric[1] é o valor da métrica um dia depois da originação
O último index corresponde à data de ontem.

Definição da métrica "latency"
A métrica latency (atraso) é uma lista de boolean, e indica se há atraso no pagamento de alguma parcela, numa determinada data.

Definição da métrica "over(n)"
On a given day, the loan has one of the three first payments overdue--"overdue" means that no payment has been received--. The number (n) indicates that the appraisal period is n days after base date.

Por exemplo:

Se a data de vencimento de uma installment é 2017-06-04, e não foi realizado nenhum pagamento dentro de 30 dias, o valor de Over30 será TRUE depois de 2017-07-04.
Uma vez que o pagamento foi recebido, o valor de over30 é FALSE
Requisitos
Você deve utilizar python, pyspark dentro do ambiante Google Colab, seguindo as intruções a seguir.

Para instalar Spark dentro de Google Colab, utilizaremos as instruções seguintes (adaptadas do blog https://movile.blog/introducao-a-spark-usando-o-google-colab/ )

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q http://www-eu.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
!tar xf spark-2.4.4-bin-hadoop2.7.tgz
!pip install -q findspark
!pip install -q pyspark
import os

os.environ["SPARK_HOME"] = "/content/spark-2.4.4-bin-hadoop2.7"
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
Após a execução deste código, será possível executar funções do Spark, por exemplo

df = spark.read.csv("localfile.csv")

Importante! O teste deve ser resolvido dentro dos limites de recursos do ambiante Google Colab.

Conteúdo do notebook
Além de transformar dados, nossos notebooks contém i) validações básicas, para ajudar nas investigações caso tenha suspeita de erros no dados e ii) análises da carteira, para servir de auxiliar no direcionamento de estudo mais detalhados.

Uma vez que a tabela loan_documentos foi gerada, queremos saber as seguintes informações:

# Desafio 1
Para cada mês de 2019, qual foi a proporção do montante recebido, em relação ao que era esperado? Ex: Vamos supor que em janeiro 2019 recebemos R$ 9000, e o valor total devido neste mês era R$ 10000, você deve criar uma query que retorne da seguinte forma:
month	year	amount	ratio
01	2019	10000.00	90%
02	2019	20000.00	80%
03	2019	50000.00	70%
04	2019	10000.00	90%
05	2019	10000.00	95%

# Desafio 2
Para cada mês de 2019, quais eram as características médias da carteira (key portfolio highlights)? Ex: Um investidor deseja saber qual é o prazo médio (avg_period), taxa de juros média (avg_interest_rate) e dia de vencimento mais frequente (freq_payday), e para isso deverá ser construída uma query com os seguintes campos:
month	year	avg_period	avg_interest_ratio	freq_payday
01	2019	10.8	7.9%	15
02	2019	9.9	8.5%	15
03	2019	10.5	8.0%	20
04	2019	10.6	7.8%	15
05	2019	10.0	7.5%	20

# Desafio 3
Para o portfolio como um todo, como se dá a distribuição dos contratos entre os diferentes prazos e taxas? Ex: Do total de contratos (loan_id), quantos possuem 6 meses de duração e taxa de 5.74%, ou 12 meses de duração e 5.15% de juros? A query deve retornar uma matriz semelhante a exemplificada abaixo:
period	interest_rate	count_loan_id	avg_interest_ratio	freq_payday
06	3.12	20		
09	3.12	17		
12	3.12	08		
06	5.15	00		
09	5.15	2		

# Desafio 4
Qual a proporção de empréstimos maturados da carteira (vencimento da primeira parcela ocorreu há pelo menos 30 dias) que estão hoje com Over30 = TRUE? Ex: Vamos supor que a carteira está crescendo rapidamente, chegando a um total de 15.000 contratos (loan_id), sendo que apenas 10.000 já passaram pelo primeiro vencimento há pelo menos 30 dias e 3.500 encontram-se em atraso. Sua query deverá retornar o seguinte resultado:
total_portfolio	matured_portfolio	over30_true	ratio
15.000	10.000	3.500	35%
O resultado poderá ser representado por gráficos da sua escolha, e aparecer dentro do notebook, usando as bibliotecas que quiser.

# Entrega:

Link do Notebook do Colab:   https://colab.research.google.com/drive/1wTvpkUmGDpBei7EvTtyFgCUkpcAwhBlA#scrollTo=9e0XrdL3NpvP
