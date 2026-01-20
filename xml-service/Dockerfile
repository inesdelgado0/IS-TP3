FROM golang:1.24-alpine AS builder

# OBRIGATÓRIO: Instalar ferramentas para compilar código C (CGO)
# pkgconfig e libxml2-dev são fundamentais para a lib que adicionaste
RUN apk add --no-cache \
    pkgconfig \
    libxml2-dev \
    gcc \
    musl-dev

ENV GOTOOLCHAIN=auto
ENV GO111MODULE=on 
# OBRIGATÓRIO para usar a libxml2
ENV CGO_ENABLED=1

WORKDIR /app

# 1. Copia o código
COPY . .

# 2. Resolve as dependências
RUN go mod tidy

# 3. Compila o binário
RUN go build -o xml-service .

# Estágio final
FROM alpine:latest

# OBRIGATÓRIO: Instalar a libxml2 no sistema final para o binário correr
RUN apk add --no-cache libxml2

WORKDIR /root/

# Copia o binário e os ficheiros necessários
COPY --from=builder /app/xml-service .
COPY --from=builder /app/.env .
# OBRIGATÓRIO: O ficheiro XSD tem de ir para dentro do container!
COPY --from=builder /app/schema.xsd .

EXPOSE 8080 50051

CMD ["./xml-service"]