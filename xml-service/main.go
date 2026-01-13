package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"xml-service/pb"
)


type server struct {
	pb.UnimplementedBIQueryServiceServer
	db *sql.DB
}

// Implementações gRPC (Requisito 14 - XPath)
func (s *server) GetMarcaStats(ctx context.Context, in *pb.Filtro) (*pb.MarcaStats, error) {
	total, preco, kms := GetMarcaStatsXPath(s.db, in.GetTermo())
	return &pb.MarcaStats{Total: total, MediaPreco: preco, MediaKms: kms}, nil
}

func (s *server) GetContagemSegmento(ctx context.Context, in *pb.Filtro) (*pb.Resultado, error) {
	total := GetCountSegmentoXPath(s.db, in.GetTermo())
	return &pb.Resultado{Valor: float32(total)}, nil
}

func (s *server) GetLocalizacaoStats(ctx context.Context, in *pb.Filtro) (*pb.LocalizacaoStats, error) {
	total, valor := GetLocalizacaoStatsXPath(s.db, in.GetTermo())
	return &pb.LocalizacaoStats{TotalCarros: total, ValorTotal: valor}, nil
}

func callWebhook(url string, reqID string, status string, fileName string) {
	data := WebhookResponse{RequestId: reqID, Status: status, FileName: fileName}
	jsonData, _ := json.Marshal(data)
	_, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("! Erro Webhook: %v\n", err)
		return
	}
	fmt.Printf(">\nWebhook avisado [%s]: %s\n", status, fileName)
}

// --- NOVO: Função de Validação (Requisito 3) ---
func validar(lista ListaVeiculos) (bool, string) {
	if len(lista.Stock) == 0 {
		return false, "ERRO_VALIDACAO: Ficheiro sem veículos para processar"
	}
	for _, v := range lista.Stock {
		if v.Identificador == "" || v.Identificacao.Preco <= 0 {
			return false, "ERRO_VALIDACAO: Dados obrigatórios (ID/Preço) inválidos ou ausentes"
		}
	}
	return true, "SUCCESS"
}


func main() {
	godotenv.Load()
	db := ConnectDB()
	defer db.Close()

	// 1. Servidor gRPC (Requisito 8d)
	go func() {
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("Falha gRPC: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterBIQueryServiceServer(s, &server{db: db})
		fmt.Println("\nServidor gRPC ON na porta 50051")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Erro gRPC: %v", err)
		}
	}()

	// 2. Endpoint REST para Upload (Requisito 8a)
	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		reqID := r.FormValue("requestId")
		mapperVer := r.FormValue("mapper")
		webhookURL := r.FormValue("webhookUrl")
		fileName := r.FormValue("fileName")

		file, _, err := r.FormFile("csvFile")
		if err != nil {
			http.Error(w, "Erro ao receber ficheiro", 400)
			return
		}

		var buf bytes.Buffer
		buf.ReadFrom(file)
		file.Close()

		go func(csvData []byte, id, fname, wURL, mVer string) {
			reader := csv.NewReader(bytes.NewReader(csvData))
			reader.LazyQuotes = true
			linhas, err := reader.ReadAll()
			if err != nil {
				callWebhook(wURL, id, "ERRO_CSV", fname)
				return
			}

			// --- AJUSTE AQUI: Preenchimento conforme o exemplo do professor ---
			relatorio := ListaVeiculos{
				DataGeracao: time.Now().Format("2006-01-02"),
				Versao:      "1.0", // Versão do esquema
				Stock:       []VeiculoXML{},
			}
			// Usando o ID dinâmico nos atributos de configuração
			relatorio.Configuracao.ValidadoPor = "XML_Service_ID_" + id
			relatorio.Configuracao.Requisitante = "Processador_ID_" + id

			for i, col := range linhas {
				if i == 0 || len(col) < 13 { continue }

				v := VeiculoXML{}
				v.Identificador = col[0]
				v.Identificacao.Designacao = col[1]
				v.Identificacao.Preco, _ = strconv.ParseFloat(col[2], 64)
				v.Identificacao.Ano, _ = strconv.Atoi(col[3])
				v.Identificacao.CategoriaVeiculo = col[10]
				v.DetalhesTecnicos.Cilindrada, _ = strconv.Atoi(col[7])
				v.DetalhesTecnicos.PotenciaMotor, _ = strconv.Atoi(col[8])
				v.DetalhesTecnicos.TipoCombustivel = col[5]
				v.DetalhesTecnicos.TipoTransmissao = col[9]
				v.HistoricoUso.Kilometragem, _ = strconv.Atoi(col[4])
				v.Geografia.Cidade = col[6]
				v.Geografia.GPS.Lat, _ = strconv.ParseFloat(col[11], 64)
				v.Geografia.GPS.Lon, _ = strconv.ParseFloat(col[12], 64)

				relatorio.Stock = append(relatorio.Stock, v)
			}

			// Validação e Persistência
			ok, status := validar(relatorio)
			if ok {
				xmlBytes, _ := xml.MarshalIndent(relatorio, "", "  ")
				xmlFinal := string(xml.Header) + string(xmlBytes)

				err := SaveXML(db, xmlFinal, mVer)
				if err != nil {
					status = "ERRO_PERSISTENCIA"
				}
			}

			if wURL != "" {
				callWebhook(wURL, id, status, fname)
			}
		}(buf.Bytes(), reqID, fileName, webhookURL, mapperVer)

		w.WriteHeader(http.StatusAccepted)
	})

	fmt.Println("\nServiço XML ON na porta 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}