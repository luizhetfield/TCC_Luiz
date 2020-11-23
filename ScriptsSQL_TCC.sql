###
### Pré processamento da base de Ouvidoria
###

#1 Excluir da base os registros cujo id_cliente seja omisso
DELETE FROM puc.ouvidoria WHERE id_cliente ="";

#2 Excluir da base, as ocorrências cuja origem não integram o objetivo do trabalho (conduções internas, pedidos de informação realizados 
#  por órgãos diversos, protocolos vinculados a esteira de judicialização, etc.)
DELETE FROM puc.ouvidoria WHERE Origem not in ('Bacen', 'Ouvidoria', 'Outras Ouvidorias', 'Exclusivo Ouvidoria', 'Procon');

#3 - Acertar os campos que contém informaçao do tipo "data", e que se encontram definidos como "text" evitando-se possíveis conflitos,
# assim como os campos int importados como text, para evitar conflito na importação

ALTER TABLE puc.ouvidoria ADD COLUMN novaDataRegistro date;
ALTER TABLE puc.ouvidoria ADD COLUMN novaDataSolucao date;
UPDATE puc.ouvidoria
SET novaDataRegistro = str_to_date(Data_Registro, '%d/%m/%Y');
UPDATE puc.ouvidoria
SET novaDataSolucao = str_to_date(Data_Solucao, '%d/%m/%Y');
ALTER TABLE puc.ouvidoria DROP COLUMN Data_Registro;
ALTER TABLE puc.ouvidoria DROP COLUMN Data_Solucao;
ALTER TABLE puc.ouvidoria MODIFY COLUMN id_ocorrencia int;
ALTER TABLE puc.ouvidoria MODIFY COLUMN Cod_Assunto int;
ALTER TABLE puc.ouvidoria MODIFY COLUMN Cod_Produto int;
ALTER TABLE puc.ouvidoria MODIFY COLUMN id_cliente int;
ALTER TABLE puc.ouvidoria MODIFY COLUMN Gestor int;

/*ALTER TABLE puc.ouvidoria MODIFY COLUMN Gestor int;
  Ao tentar transformar a coluna "Gestor" em int, recebi mensagem de erro, informando que havia valores incompatíveis.
  Ao explorar a tabela através do Pandas, percebi que haviam campos null (o que, no Pandas, é chamado de NaN).
  Pelo próprio Pandas, defini os NaN como 0 (zeros) e realizei o ajuste a partir dos Códigos de Assuntos sob responsabilidades dos
  Gestores ("Cod_Assunto"). O código está mais adiante, ainda neste script.
  */
  
#4 - Anonimização do identificador cadastral interno dos clientes

CREATE TABLE id_distinct AS (SELECT DISTINCT id_cliente FROM puc.ouvidoria);
ALTER TABLE id_distinct ADD COLUMN id INT NOT NULL AUTO_INCREMENT PRIMARY KEY;
ALTER TABLE ouvidoria ADD COLUMN id int;
UPDATE ouvidoria, id_distinct
SET ouvidoria.id = id_distinct.id
WHERE ouvidoria.id_cliente = id_distinct.id_cliente;
ALTER TABLE ouvidoria DROP COLUMN id_cliente;

#5 - Anonimização do número de protocolo interno, vinculado à ocorrencia aberta pelo cliente

ALTER TABLE ouvidoria ADD COLUMN id_protocolo int NOT NULL AUTO_INCREMENT PRIMARY KEY;
ALTER TABLE ouvidoria ADD COLUMN ocorrencia varchar(10);
UPDATE ouvidoria
SET ocorrencia = concat('ocorr', id_protocolo);
ALTER TABLE ouvidoria DROP COLUMN id_ocorrencia;
ALTER TABLE ouvidoria DROP COLUMN id_protocolo;

#6 - Exclui ocorrências Bacen abertas "direto", ou seja, sem acionamento prévio da Ouvidoria

DELETE o
FROM ouvidoria o
INNER JOIN (
    SELECT id, min(novaDataRegistro) primeiradata
    FROM ouvidoria 
    WHERE Origem = 'Bacen' 
    GROUP BY id
) j ON o.id = j.id and o.novaDataRegistro > j.primeiradata;

#7 - Cria atributo "contagem", referente à quantidade de ocorrências abertas pelo cliente
ALTER TABLE ouvidoria ADD COLUMN contagem int;
UPDATE ouvidoria o
INNER JOIN(
SELECT id, count(ocorrencia) as cont
FROM ouvidoria
GROUP BY id
) j1
SET o.contagem = j1.cont
WHERE o.id = j1.id;

#8 - Label Encoding para clientes com Bacen, trabalhando a informação de forma binária (1 = possui Bacen, 2= não possui Bacen)
ALTER TABLE ouvidoria ADD COLUMN Bacen int;

UPDATE ouvidoria
INNER JOIN (
SELECT id
FROM ouvidoria
WHERE Origem = 'Bacen'
GROUP BY id
)j1 on j1.id = ouvidoria.id
SET Bacen = 1 
WHERE j1.id = ouvidoria.id;

UPDATE ouvidoria 
SET Bacen = 0
WHERE Bacen is null or Bacen = '';

DELETE FROM ouvidoria
WHERE Origem = 'Bacen';

#9 - Criar prazo_medio computando o prazo médio de resposta às demandas registradas pelo cliente

ALTER TABLE ouvidoria ADD COLUMN dias int;
UPDATE ouvidoria
INNER JOIN(
SELECT id, ocorrencia, novaDataRegistro, novaDataSolucao
FROM ouvidoria
)j1 on j1.id = ouvidoria.id
SET ouvidoria.dias = (j1.novaDataSolucao - j1.novaDataRegistro)
WHERE ouvidoria.ocorrencia = j1.ocorrencia ;


ALTER TABLE ouvidoria ADD COLUMN prazo_medio int;
UPDATE ouvidoria
INNER JOIN(
SELECT id, round(avg(dias)) as media
FROM ouvidoria
GROUP BY id
)j1 on j1.id = ouvidoria.id
SET ouvidoria.prazo_medio = j1.media;

ALTER TABLE ouvidoria DROP COLUMN dias;

#10 - Label Encoding das Ufs

ALTER TABLE ouvidoria ADD COLUMN uf_id int;

UPDATE ouvidoria
INNER JOIN(
SELECT uf, id 
FROM ufs
)j1 on j1.uf = ouvidoria.UF_Rel
SET ouvidoria.uf_id = j1.id;
ALTER TABLE ouvidoria DROP COLUMN UF_Rel;

#11 Label Encoding para julgamento de Solução

ALTER TABLE ouvidoria ADD COLUMN sol int;

UPDATE ouvidoria
INNER JOIN (
SELECT id
FROM ouvidoria
WHERE Solucao = 'Soluci.'
GROUP BY id
)j1 on j1.id = ouvidoria.id
SET sol = 1 
WHERE j1.id = ouvidoria.id;

UPDATE ouvidoria 
SET sol = 0
WHERE sol is null or sol = '';

ALTER TABLE ouvidoria DROP COLUMN Solucao;

#12 - Foram identificados 6676  campos de Gestor do Produto (Gestor) sem registro. Tal situação pode ser resolvida
#    de forma segura, a partir dos Códigos de Assunto (Cod_Assunto), uma vez que todo assunto é de responsabilidade
#    de um gestor específico. Após o ajuste, pode-se ajustar o tipo de dado do campo Gestor

CREATE TABLE assunto AS SELECT DISTINCT Cod_Assunto, Gestor FROM ouvidoria WHERE Gestor is not null;

UPDATE ouvidoria, assunto
SET ouvidoria.Gestor = assunto.Gestor
WHERE ouvidoria.Gestor is null;

ALTER TABLE ouvidoria MODIFY COLUMN Gestor int;

#13 - Extração dos assuntos, produtos e Gestores mais reclamados pelo cliente para observação pelo modelo

UPDATE ouvidoria
JOIN(
SELECT id, Cod_Assunto, max(y.conta_assunto) FROM(
SELECT id, Cod_Assunto, count(*) as conta_assunto
from ouvidoria
GROUP BY id, Cod_Assunto
ORDER BY conta_assunto DESC)y
GROUP BY id)j on ouvidoria.id = j.id
SET ouvidoria.Cod_Assunto = j.Cod_Assunto
WHERE ouvidoria.id = j.id;

/* Variável Produto desconsiderada

UPDATE ouvidoria
JOIN(
SELECT id, Cod_Produto, max(y.conta_produto) FROM(
SELECT id, Cod_Produto, count(*) as conta_produto
from ouvidoria
GROUP BY id, Cod_Produto
ORDER BY conta_produto DESC)y
GROUP BY id)j on ouvidoria.id = j.id
SET ouvidoria.Cod_Produto = j.Cod_Produto
WHERE ouvidoria.id = j.id;
*/


UPDATE ouvidoria
JOIN(
SELECT id, Gestor, max(y.conta_gestor) FROM(
SELECT id, Gestor, count(*) as conta_gestor
from ouvidoria
GROUP BY id, Gestor
ORDER BY conta_gestor DESC)y
GROUP BY id)j on ouvidoria.id = j.id
SET ouvidoria.Gestor = j.Gestor
WHERE ouvidoria.id = j.id;


###
### Pré processamento da base de dados cadastrais
###

#1 Mascaramento do identificador cadastral interno dos clientes
ALTER TABLE cadastro MODIFY COLUMN id_cliente int;

UPDATE cadastro c
INNER JOIN (
SELECT id_cliente, id 
FROM puc.id_distinct
)j1 on c.id_cliente = j1.id_cliente
SET c.id_cliente = j1.id
WHERE c.id_cliente = j1.id_cliente;

#2 Ajuste do campo renda que, na base original, foi registrado como texto

UPDATE puc.cadastro
SET VL_REND_LQDO = REPLACE(VL_REND_LQDO, ',', '.');

ALTER TABLE puc.cadastro MODIFY COLUMN VL_REND_LQDO decimal(10,2);

ALTER TABLE puc.cadastro MODIFY COLUMN VL_REND_LQDO int;

# De forma a normalizar o campo Renda, optei por trabalhar o mesmo por faixas, utilizando os mesmos critérios de definição de 
# Classe Econômica do IBGE- conceito este aplicado na Pesquisa de Orçamentos Familiares realizada periodicamente por aquela 
# Instituiçao (edição 2017/2018 disponívem em "https://biblioteca.ibge.gov.br/visualizacao/livros/liv101670.pdf"
#                   Rendimentos (R$)
# Classe A         Até 1908
# Classe B         Mais de 1908 a 2862
# Classe C         Mais de 2862 a 5724
# Classe D         Mais de 5724 a 9540
# Classe E         Mais de 9540 a 14310
# Classe F         Mais de 14310 a 23850
# Classe G         Mais de 23850

UPDATE cadastro
SET VL_REND_LQDO =
CASE 
	WHEN VL_REND_LQDO BETWEEN 0 and 1908
		THEN 1
	WHEN VL_REND_LQDO BETWEEN 1909 and 2862
		THEN 2
	WHEN VL_REND_LQDO BETWEEN 2863 and 5724
		THEN 3
	WHEN VL_REND_LQDO BETWEEN 5725 and 9540
		THEN 4
	WHEN VL_REND_LQDO BETWEEN 9541 and 14310
		THEN 5
	WHEN VL_REND_LQDO BETWEEN 14311 and 23850
		THEN 6
	WHEN VL_REND_LQDO > 23850
		THEN 7
	END;
    
#3 Apenas observação: a variável COD_GRAU_INST (Grau de Instrução) já se encontra label encoded, pronta para alimentar o modelo:

#1 - Analfabeto
#2 - Ensino Fundamental
#3 - Ensino Medio
#4 - Superior Incompleto
#5 - Superior Completo
#6 - Pos Graduacao
#7 - Mestrado
#8 - Doutorado
#9 - Superior em Andamento
#0 - Não informado

#Da mesma forma, a variável COD_ETDO_CVIL:

#1 - SOLTEIRO(A)
#2 - CASADO(A) - COMUNHAO UNIVERSAL
#3 - CASADO(A) - COMUNHAO PARCIAL
#4 - CASADO(A) - SEPARACAO DE BENS
#5 - VIUVO(A)
#6 - SEPAARADO(A) JUDICIALMENTE
#7 - DIVORCIADO(A)
#0 - NAO INFORMADO

#4 O campo "TXT_CRGO", que traz a profissão do cliente, possui registro realizado de forma manual, o que favorece a total
#  inexistência de padronizaçAo, dificultando o agrupamento, uma vez que, assim, a mesma profissão é registrada de formas diferentes,
#  inclusive com diferenciação por erro de grafia. Exemplos: 

#  "PROEFESSRO ESCOLA ESTADUAL", "PROF ADJUNTO I", "PROF AUXILIAR", "PROF DE EDUCACAO BASICA", "PROF DE EDUCACAO BASICA I", etc.;
#  "AUX SER GERAIS", "AUX SERV GERAIS", "AUX SERV. GERAIS", "AUX SERVICOS GERAIS", "AUX. DE SERVICOS GERAIS", etc.;
#  "JUIZ", "JUIZ FEDERAL", "JUIZ APOSENTADO", "JUIZ DE DIREITO", "JUIZA", "JUIZA DE DIREITO", etc.;
#  "1 TEN P.M.", "1 TENENTE", "1 TENENTE PM", "1 TENETE".
#   Como forma de contornar tal situação prejudicial, uma vez que a profissão do cliente é variável de interesse neste trabalho,
#   foi decidido agrupar as profissões a partir de substring comum no prefixo do termo. Assim, 
#   "JUIZ", "JUIZ FEDERAL", "JUIZ APOSENTADO", "JUIZ DE DIREITO", "JUIZA", "JUIZA DE DIREITO" e etc., integrarão a profissão "JUIZ";
#   "PROEFESSRO ESCOLA ESTADUAL", "PROF ADJUNTO I", "PROF AUXouvidoriaILIAR", "PROF DE EDUCACAO BASICA", "PROF DE EDUCACAO BASICA I" e etc.,
#   integrarão a profissão "PROFESSOR".
#   "AUX SER GERAIS", "AUX SERV GERAIS", "AUX SERV. GERAIS", "AUX SERVICOS GERAIS", "AUX. DE SERVICOS GERAIS", etc., integrarão
#   a profissor "AUXILIAR".

#   O mesmo raciocínio será utilizado em toda a base, mas para esta tarefa, optei por utilizar o pandas

SELECT DISTINCT TXT_CRGO
FROM cadastro;

SELECT DISTINCT TXT_CRGO, NOM_EMPD
FROM cadastro
WHERE substring(TXT_CRGO, 1, 1)  in (1, 2, 3, 4, 5, 6, 7, 8, 9);

# Conforme Query acima, identificamos que, com exceção pontual de 4 nomenclaturas ("905", "5", "4GASG A", "41 APOSENT POR IDADE", 
# "136550774-0 E 171827571-1", "1800", "2200", "27 SOLDADO DO EXERCITO"), todos os casos de nome de profissão iniciando por 
#  algarismo inteiro, se tratam de Oficiais Militares, o que permite agrupá-los de forma considerada
# satisfatória. Apenas a título de informação, ao procurar pelo termo "GASG NATAL/RN", nos deparamos com a Lei municipal 
# Nº 6435, DE 12 DE FEVEREIRO DE 2014, que tratou de reajuste de servidores da secretaria de saúde daquele município, permitindo-se
# identificar, com exatidão, não se tratar de militar.

#Outra observação importante: dentre as variáveis pré-julgadas como relevantes para o modelo (como idade), havia aquelas que não 
# permitiram atribuição de valor, independentemente da estratégia pretendida (valor mais frequente, média, etc.), nos casos 
#de ausência de registro, razão pela qual entendi mais adequado a exclusão da base, no que tange aos registros nesta situação, a 
#ter a variável desconsiderada por completo.
#Neste sentido, todos os registros cuja idade estavam sem atribuição, foram excluídos.
#Da mesma forma, UF também se mostra como registro sem possibilidade de ajuste, já que não tivemos acesso, para o desenvolvimento
#do trabalho, aos dados de endereço do cliente (município).
# Já aqueles que não possuem renda terão valores atribuídos com base na renda média encontrada em pessoas da mesma idade e 
# grau de escolaridade. Inicialmente, pensei em fazer este trabalho considerando pessoas que tivessem o mesmo radical na nomenclatura
# do cargo exercido. Entretanto, tal estratégia traria desvirtuamentos, uma vez que, por exemplo, Auxiliar de Limpeza e Auxiliar de 
# Enfermagem, embora possuindo o mesmo radical na nomenclatura, apresentam disparidade significativa nas rendas auferidas. O mesmo
# entre os militares. O soldo de um soldado é muito díspar do soldo de um oficial, e assim sucessivamente.
# A estratégia de idade e grau de escolaridade se mostra como mais razoável e equilibrada.


#Consulta de viabilidade da exclusão dos registros com idade = 0

SELECT * FROM cadastro 
WHERE idade = 0 and COD_ETDO_CVIL in (1, 0) and COD_GRAU_INST in (0, 1, 2) and TXT_CRGO = "" ;

# Por fim, criação da base final de trabalho:

CREATE TABLE base_unificada AS SELECT a.id, a.Cod_Assunto, a.Gestor, a.contagem, 
a.prazo_medio, a.uf_id, a.sol, a.Bacen, b.VL_REND_LQDO, b.idade, b.COD_GRAU_INST, b.COD_ETDO_CVIL
FROM ouvidoria a, cadastro b
WHERE b.id_cliente = a.id
GROUP BY a.id










