package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"xml-service/pb"
)

type server struct {
	pb.UnimplementedBIQueryServiceServer
	db *sql.DB
}

// 1. Implementação das Stats da Marca
func (s *server) GetMarcaStats(ctx context.Context, in *pb.Filtro) (*pb.MarcaStats, error) {
	total, preco, kms := GetMarcaStatsXPath(s.db, in.GetTermo())
	return &pb.MarcaStats{
		Total:      total,
		MediaPreco: preco,
		MediaKms:   kms,
	}, nil
}

// 2. Implementação da Contagem por Segmento
func (s *server) GetContagemSegmento(ctx context.Context, in *pb.Filtro) (*pb.Resultado, error) {
	total := GetCountSegmentoXPath(s.db, in.GetTermo())
	return &pb.Resultado{Valor: float32(total)}, nil
}

// 3. Implementação das Stats por Localização
func (s *server) GetLocalizacaoStats(ctx context.Context, in *pb.Filtro) (*pb.LocalizacaoStats, error) {
	total, valor := GetLocalizacaoStatsXPath(s.db, in.GetTermo())
	return &pb.LocalizacaoStats{
		TotalCarros: total,
		ValorTotal:  valor,
	}, nil
}

func callWebhook(url string, reqID string, fileName string) {
	data := WebhookResponse{RequestId: reqID, Status: "SUCCESS", FileName: fileName}
	jsonData, _ := json.Marshal(data)
	http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	fmt.Printf("> Webhook avisado: %s\n", fileName)
}

func main() {
	godotenv.Load()
	db := ConnectDB()
	defer db.Close()

	// 1. Iniciar Servidor gRPC numa Goroutine
	go func() {
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("Falha ao ouvir gRPC: %v", err)
		}
		
		s := grpc.NewServer()
		
		// REGISTO REAL: Liga a tua struct 'server' ao gRPC
		pb.RegisterBIQueryServiceServer(s, &server{db: db})
		
		fmt.Println("Servidor gRPC (Go) ON na porta 50051...")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Falha ao servir gRPC: %v", err)
		}
	}()

	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		// 1. Parse do formulário (Limite 10MB)
		err := r.ParseMultipartForm(10 << 20)
		if err != nil {
			http.Error(w, "Erro no Parse", 400)
			return
		}

		reqID := r.FormValue("requestId")
		mapperVer := r.FormValue("mapper")
		webhookURL := r.FormValue("webhookUrl")
		fileName := r.FormValue("fileName")

		// 2. Abrir o ficheiro recebido
		file, _, err := r.FormFile("csvFile")
		if err != nil {
			fmt.Println("Erro ao receber ficheiro:", err)
			return
		}
		defer file.Close()

		// 3. Ler o CSV
		reader := csv.NewReader(file)
		reader.LazyQuotes = true
		reader.TrimLeadingSpace = true
		linhas, err := reader.ReadAll()
		
		if err != nil || len(linhas) == 0 {
			fmt.Printf("ALERTA: Recebi o ficheiro %s mas detetei %d linhas.\n", fileName, len(linhas))
			return
		}

		fmt.Printf("\n[DEBUG] A processar %d linhas de %s...\n", len(linhas), fileName)

		for i, col := range linhas {
			if i == 0 || len(col) < 12 { continue }

			v := VeiculoXML{ID_Requisicao: reqID}
			
			// Ajuste para bater com o teu XPath:
			v.Identificacao.Designacao = col[0]
			v.Identificacao.PrecoVenda, _ = strconv.ParseFloat(col[1], 64)
			v.Identificacao.AnoRegisto, _ = strconv.Atoi(col[2])
			v.Identificacao.CategoriaVeiculo = col[9]

			v.DetalhesTecnicos.Motorizacao.CilindradaCC, _ = strconv.Atoi(col[6])
			v.DetalhesTecnicos.Motorizacao.PotenciaMotor, _ = strconv.Atoi(col[7])
			v.DetalhesTecnicos.HistoricoUso.Kilometragem, _ = strconv.Atoi(col[3])
			v.DetalhesTecnicos.HistoricoUso.FonteEnergia = col[4]
			v.DetalhesTecnicos.HistoricoUso.TipoTransmissao = col[8]

			v.Geografia.Cidade = col[5]
			v.Geografia.PosicionamentoGPS.Lat, _ = strconv.ParseFloat(col[10], 64)
			v.Geografia.PosicionamentoGPS.Lon, _ = strconv.ParseFloat(col[11], 64)

			xmlBytes, _ := xml.MarshalIndent(v, "", "  ")
			SaveXML(db, string(xml.Header)+string(xmlBytes), mapperVer)
		}

		if webhookURL != "" {
			callWebhook(webhookURL, reqID, fileName)
		}
		w.WriteHeader(http.StatusOK)
	})

	fmt.Println("Serviço XML (Go) ON na porta 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}