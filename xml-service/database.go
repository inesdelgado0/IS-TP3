package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func ConnectDB() *sql.DB {
	connStr := os.Getenv("DATABASE_URL")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Erro na configuração da base de dados:", err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("\nNão foi possível ligar ao PostgreSQL. Verifica o .env: ", err)
	}

	fmt.Println("\nConectado ao PostgreSQL com sucesso!")
	return db
}


func SaveXML(db *sql.DB, xmlDoc string, mapperVer string) error {
	query := `INSERT INTO veiculos_xml (xml_documento, data_criacao, mapper_version) VALUES ($1, $2, $3)`
	_, err := db.Exec(query, xmlDoc, time.Now(), mapperVer)
	if err != nil {
		log.Println("Erro ao inserir XML:", err)
		return err
	}
	log.Println("XML guardado na base de dados local.")
	return nil
}


func GetMarcaStatsXPath(db *sql.DB, marca string) (int32, float32, float32) {
	var total int32
	var mPreco, mKms sql.NullFloat64

	query := `
		WITH v_table AS (
			SELECT 
				unnest(xpath('/RelatorioVeiculos/Stock/Veiculo/@IDInterno', xml_documento))::text as id_interno,
				unnest(xpath('/RelatorioVeiculos/Stock/Veiculo/Identificacao/Designacao/text()', xml_documento))::text as designacao,
				unnest(xpath('/RelatorioVeiculos/Stock/Veiculo/Identificacao/Preco/text()', xml_documento))::text::numeric as preco,
				unnest(xpath('/RelatorioVeiculos/Stock/Veiculo/HistoricoUso/Kilometragem/text()', xml_documento))::text::numeric as kms
			FROM veiculos_xml
		),
		deduplicated AS (
			-- DISTINCT ON garante que se o mesmo IDInterno aparecer em vários XMLs, só contamos uma vez
			SELECT DISTINCT ON (id_interno) id_interno, designacao, preco, kms
			FROM v_table
			WHERE designacao ILIKE $1
		)
		SELECT 
			COUNT(*),
			COALESCE(AVG(preco), 0),
			COALESCE(AVG(kms), 0)
		FROM deduplicated`
	
	err := db.QueryRow(query, "%"+marca+"%").Scan(&total, &mPreco, &mKms)
	if err != nil {
		log.Println("Erro XPath Marca:", err)
		return 0, 0, 0
	}
	return total, float32(mPreco.Float64), float32(mKms.Float64)
}


func GetCountSegmentoXPath(db *sql.DB, segmento string) int32 {
	var total int32
	query := `
		WITH v_table AS (
			SELECT 
				unnest(xpath('/RelatorioVeiculos/Stock/Veiculo/@IDInterno', xml_documento))::text as id_interno,
				unnest(xpath('/RelatorioVeiculos/Stock/Veiculo/Identificacao/Categoria/text()', xml_documento))::text as cat
			FROM veiculos_xml
		)
		SELECT COUNT(DISTINCT id_interno) 
		FROM v_table 
		WHERE cat ILIKE $1`
	
	err := db.QueryRow(query, "%"+segmento+"%").Scan(&total)
	if err != nil {
		log.Println("Erro XPath Segmento:", err)
		return 0
	}
	return total
}


func GetLocalizacaoStatsXPath(db *sql.DB, cidade string) (int32, float32) {
	var total int32
	var valorTotal sql.NullFloat64

	query := `
		WITH v_table AS (
			SELECT 
				unnest(xpath('/RelatorioVeiculos/Stock/Veiculo/@IDInterno', xml_documento))::text as id_interno,
				unnest(xpath('/RelatorioVeiculos/Stock/Veiculo/Geografia/Cidade/text()', xml_documento))::text as cidade_nome,
				unnest(xpath('/RelatorioVeiculos/Stock/Veiculo/Identificacao/Preco/text()', xml_documento))::text::numeric as preco
			FROM veiculos_xml
		),
		deduplicated AS (
			SELECT DISTINCT ON (id_interno) id_interno, cidade_nome, preco
			FROM v_table
			WHERE cidade_nome ILIKE $1
		)
		SELECT 
			COUNT(*),
			COALESCE(SUM(preco), 0)
		FROM deduplicated`

	err := db.QueryRow(query, "%"+cidade+"%").Scan(&total, &valorTotal)
	if err != nil {
		log.Println("Erro XPath Localidade:", err)
		return 0, 0
	}
	return total, float32(valorTotal.Float64)
}