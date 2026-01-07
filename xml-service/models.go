package main

import "encoding/xml"

type WebhookResponse struct {
    RequestId string `json:"requestId"`
    Status    string `json:"status"`
    FileName  string `json:"fileName"`
}

type VeiculoXML struct {
    XMLName       xml.Name `xml:"Veiculo"`
    ID_Requisicao string   `xml:"id_transacao,attr"`
    
    // Mudamos de InfoGeral para Identificacao para bater com o main.go
    Identificacao struct {
        Designacao       string  `xml:"Designacao"`
        PrecoVenda       float64 `xml:"PrecoVenda"`
        AnoRegisto       int     `xml:"AnoRegisto"`
        CategoriaVeiculo string  `xml:"CategoriaVeiculo"`
    } `xml:"Identificacao"`

    // Mudamos de Especificacoes para DetalhesTecnicos
    DetalhesTecnicos struct {
        Motorizacao struct {
            CilindradaCC  int `xml:"CilindradaCC"`
            PotenciaMotor int `xml:"PotenciaMotor"`
        } `xml:"Motorizacao"`
        
        HistoricoUso struct {
            Kilometragem    int    `xml:"Kilometragem"`
            FonteEnergia    string `xml:"FonteEnergia"`
            TipoTransmissao string `xml:"TipoTransmissao"`
        } `xml:"HistoricoUso"`
    } `xml:"DetalhesTecnicos"`

    // Mudamos de Localizacao para Geografia
    Geografia struct {
        Cidade           string `xml:"Cidade"`
        PosicionamentoGPS struct {
            Lat float64 `xml:"Lat"`
            Lon float64 `xml:"Lon"`
        } `xml:"PosicionamentoGPS"`
    } `xml:"Geografia"`
}