require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');
const csv = require('csv-parser');
const { Readable } = require('stream');
const express = require('express');
const FormData = require('form-data');
const fs = require('fs');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const BUCKET_NAME = 'carros';
const PORT_WEBHOOK = 3000;

// VARIÁVEL DE CONTROLO 
let isProcessing = false;

const MAPPER_CONFIG = {
    version: "1.0"
};

function aplicarMapper(dadosOriginais, coords) {
    return {
        "Identificador": dadosOriginais.id_externo || "N/A", 
        "Designacao": dadosOriginais.marca_modelo || "N/A",
        "Preco": dadosOriginais.preco_eur || "0",
        "Ano": dadosOriginais.ano || "0",
        "Kilometragem": dadosOriginais.quilometros || "0",
        "TipoCombustivel": dadosOriginais.combustivel || "N/A",
        "Cidade": dadosOriginais.localidade || "N/A",
        "Cilindrada": dadosOriginais.cilindrada || "0",
        "PotenciaMotor": dadosOriginais.potencia || "0",
        "TipoTransmissao": dadosOriginais.caixa || "N/A",
        "CategoriaVeiculo": dadosOriginais.segmento || "N/A",
        "Latitude": coords.lat,
        "Longitude": coords.lon
    };
}

async function getGPS(localidade) {
    if (!localidade || localidade === "N/A") return { lat: "0.0", lon: "0.0" };
    try {
        const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(localidade + ", Portugal")}&format=json&limit=1`;
        const response = await axios.get(url, { headers: { 'User-Agent': 'Trabalho_Final_Carros' } });
        if (response.data && response.data.length > 0) {
            return { lat: response.data[0].lat, lon: response.data[0].lon };
        }
    } catch (error) {
        console.error(`! Erro GPS [${localidade}]`);
    }
    return { lat: "0.0", lon: "0.0" };
}


async function enviarParaXMLService(dados, mapperVersion, fileName) {
    try {
        const form = new FormData();
        
        // Garante que pegamos as colunas na ordem certa
        const headers = Object.keys(dados[0]).join(',');
        const rows = dados.map(row => 
            Object.values(row).map(val => {
                // Limpeza básica para evitar que vírgulas nos nomes quebrem o CSV
                let s = String(val).replace(/,/g, " "); 
                return `"${s}"`;
            }).join(',')
        ).join('\r\n');
        
        const csvFinal = headers + '\r\n' + rows + '\r\n';

        // --- BLOCO DE DEBUG: GUARDAR LOCALMENTE ---
        try {
            const debugFileName = `debug_${fileName}`;
            fs.writeFileSync(debugFileName, csvFinal);
            console.log(`\n[DEBUG] Ficheiro guardado localmente como: ${debugFileName}`);
        } catch (fsErr) {
            console.error("Erro ao guardar ficheiro de debug:", fsErr.message);
        }
        // ------------------------------------------


        const buffer = Buffer.from(csvFinal, 'utf-8');

        form.append('requestId', `REQ-${Date.now()}`);
        form.append('mapper', mapperVersion);
        form.append('fileName', fileName);
        form.append('webhookUrl', `http://host.docker.internal:${PORT_WEBHOOK}/webhook`);
        form.append('csvFile', buffer, { filename: fileName, contentType: 'text/csv' });

        console.log(`\n> Enviando ${dados.length} linhas para o Go...`);
        await axios.post('http://localhost:8080/upload', form, { headers: form.getHeaders() });
    } catch (error) {
        console.error("! Erro envio:", error.message);
    }
}

async function processFile(fileName) {
    if (isProcessing) return; // BLOQUEIA SE JÁ ESTIVER A TRABALHAR
    isProcessing = true;

    console.log(`\n--- Encontrou na supabase: ${fileName} ---`);
    
    try {
        const { data, error } = await supabase.storage.from(BUCKET_NAME).download(fileName);
        if (error) throw error;

        const results = [];
        const buffer = Buffer.from(await data.arrayBuffer());
        const stream = Readable.from(buffer);

        stream.pipe(csv())
            .on('data', (row) => results.push(row))
            .on('end', async () => {
                console.log(`\nTotal a complementar com a API de coordenadas: ${results.length} linhas.`);

                const dadosProntos = [];
                for (let i = 0; i < results.length; i++) {
                    const coords = await getGPS(results[i].localidade);
                    dadosProntos.push(aplicarMapper(results[i], coords));
                    
                    // LOG DE PROGRESSO (Para saberes que não parou)
                    if (i % 20 === 0) process.stdout.write(`\rProcessado: ${i}/${results.length}...`);
                    
                    // Delay menor para ser mais rápido mas não ser banido
                    await new Promise(res => setTimeout(res, 300));
                }

                console.log("\nTransformação concluída.");
                await enviarParaXMLService(dadosProntos, MAPPER_CONFIG.version, fileName);
                
                isProcessing = false; 
            });
    } catch (err) {
        console.error("Erro:", err);
        isProcessing = false;
    }
}

async function pollingBucket() {
    if (isProcessing) return; // Se já está a processar um, não faz nada agora
    
    console.log("\nPolling: A verificar ficheiros na Supabase...");
    const { data, error } = await supabase.storage.from(BUCKET_NAME).list();
    if (error || !data) return;

    // Filtramos e ordenamos: o mais antigo (criado há mais tempo) aparece primeiro
    const files = data
        .filter(f => f.name.startsWith('carros_'))
        .sort((a, b) => new Date(a.created_at) - new Date(b.created_at));

    if (files.length > 0) {
        // Pega no primeiro (o mais antigo da fila)
        await processFile(files[0].name);
    } else {
        console.log("Nenhum ficheiro pendente no bucket.");
    }
}

const app = express();
app.use(express.json());

app.post('/webhook', async (req, res) => {
    const { requestId, status, fileName } = req.body;
    console.log(`\n[WEBHOOK] Recebido status ${status} para o pedido ${requestId}`);
    if (status === 'SUCCESS' || status === 'OK') {
        const { error } = await supabase.storage.from(BUCKET_NAME).remove([fileName]);
        if (!error) console.log(`Ficheiro ${fileName} limpo do Storage.`);
    }
    res.sendStatus(200);
});


app.listen(PORT_WEBHOOK, () => {
    console.log(`\nServidor Webhook a correr na porta ${PORT_WEBHOOK}`);
    
    pollingBucket();
});

setInterval(pollingBucket, 30000);