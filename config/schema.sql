
CREATE database escolas;


CREATE TABLE enderecos (
	id serial,
	municipio varchar(30),
	nome_escola	varchar(100),
	endereco_escola varchar(60),
	numero_endereco	varchar(20),
	cep integer,
	bairro varchar(57),
	longitude varchar(12),
	latitude varchar(12)
);

CREATE TABLE enderecos (
	id serial,
	nome_rede_ensino varchar(20),
	diretoria_ensino varchar(30),
	municipio varchar(30),
	distrito varchar(35),
	cod_escola varchar(10),
	nome_escola	varchar(100),
	situacao_escola varchar(10),
	tipo_escola varchar(40),
	endereco_escola varchar(60),
	numero_endereco	integer,
	complemento varchar(50),
	cep integer,
	bairro varchar(57),
	zona_urbana varchar(6),
	longitude varchar(12),
	latitude varchar(12),
	cod_vinculadora	integer
);