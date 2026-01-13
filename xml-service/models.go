package main

import "encoding/xml"

type WebhookResponse struct {
    RequestId string `json:"requestId"`
    Status    string `json:"status"`
    FileName  string `json:"fileName"`
}


type ListaVeiculos struct {
	XMLName      xml.Name `xml:"RelatorioVeiculos"`
	DataGeracao  string   `xml:"DataGeracao,attr"`
	Versao       string   `xml:"Versao,attr"`

	// Metadata do processo (Requisito de rastreabilidade)
	Configuracao struct {
		ValidadoPor string `xml:"ValidadoPor, attr"` // XML_Service
		Requisitante string `xml:"Requisitante, attr"` // ID da Transação do Node
	} `xml:"Configuracao"`

	// Onde os carros realmente entram
	Stock []VeiculoXML `xml:"Stock>Veiculo"`
}

type VeiculoXML struct {
	Identificador string `xml:"IDInterno,attr"` // Como o Ativo IDInterno do prof
	
	Identificacao struct {
		Designacao       string  `xml:"Designacao"`
		Preco            float64 `xml:"Preco"`
		Ano              int     `xml:"Ano"`
		CategoriaVeiculo string  `xml:"Categoria"`
	} `xml:"Identificacao"`

	DetalhesTecnicos struct {
		Cilindrada      int    `xml:"Cilindrada"`
		PotenciaMotor   int    `xml:"PotenciaMotor"`
		TipoCombustivel string `xml:"TipoCombustivel"`
		TipoTransmissao string `xml:"TipoTransmissao"`
	} `xml:"DetalhesTecnicos"`

	HistoricoUso struct {
		Kilometragem int `xml:"Kilometragem"`
	} `xml:"HistoricoUso"`

	Geografia struct {
		Cidade string `xml:"Cidade"`
		GPS    struct {
			Lat float64 `xml:"Lat,attr"`
			Lon float64 `xml:"Lon,attr"`
		} `xml:"PosicionamentoGPS"`
	} `xml:"Geografia"`
}