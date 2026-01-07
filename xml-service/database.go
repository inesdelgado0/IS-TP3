package main

import (
	"database/sql"
	"log"
	"os"
	"time"
	_ "github.com/lib/pq"
)

func ConnectDB() *sql.DB {
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal("Erro ao ligar ao SupaBase:", err)
	}
	return db
}

func SaveXML(db *sql.DB, xmlDoc string, mapperVer string) {
	query := `INSERT INTO veiculos_xml (xml_documento, data_criacao, mapper_version) VALUES ($1, $2, $3)`
	_, err := db.Exec(query, xmlDoc, time.Now(), mapperVer)
	if err != nil {
		log.Println("ERRO NO SUPABASE:", err)
	} else {
		log.Println("XML INSERIDO NO SUPABASE")
	}
}

// 1. Stats por Marca (Média de Preço e KMS)
func GetMarcaStatsXPath(db *sql.DB, marca string) (int32, float32, float32) {
	var total int32
	var mPreco, mKms sql.NullFloat64

	query := `
		SELECT 
			COUNT(*),
			AVG(CAST((xpath('/Veiculo/Identificacao/PrecoVenda/text()', xml_documento))[1] AS TEXT)::NUMERIC),
			AVG(CAST((xpath('/Veiculo/DetalhesTecnicos/HistoricoUso/Kilometragem/text()', xml_documento))[1] AS TEXT)::NUMERIC)
		FROM veiculos_xml
		WHERE CAST((xpath('/Veiculo/Identificacao/Designacao/text()', xml_documento))[1] AS TEXT) ILIKE $1`

	err := db.QueryRow(query, "%"+marca+"%").Scan(&total, &mPreco, &mKms)
	if err != nil {
		return 0, 0, 0
	}
	return total, float32(mPreco.Float64), float32(mKms.Float64)
}

// 2. Contagem por Segmento (Categoria)
func GetCountSegmentoXPath(db *sql.DB, segmento string) int32 {
	var total int32
	query := `
		SELECT COUNT(*)
		FROM veiculos_xml
		WHERE CAST((xpath('/Veiculo/Identificacao/CategoriaVeiculo/text()', xml_documento))[1] AS TEXT) ILIKE $1`
	
	db.QueryRow(query, "%"+segmento+"%").Scan(&total)
	return total
}

// 3. Stats por Localização (Cidade)
func GetLocalizacaoStatsXPath(db *sql.DB, cidade string) (int32, float32) {
	var total int32
	var valorTotal sql.NullFloat64

	query := `
		SELECT 
			COUNT(*),
			SUM(CAST((xpath('/Veiculo/Identificacao/PrecoVenda/text()', xml_documento))[1] AS TEXT)::NUMERIC)
		FROM veiculos_xml
		WHERE CAST((xpath('/Veiculo/Geografia/Cidade/text()', xml_documento))[1] AS TEXT) ILIKE $1`

	db.QueryRow(query, "%"+cidade+"%").Scan(&total, &valorTotal)
	return total, float32(valorTotal.Float64)
}