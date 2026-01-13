FROM golang:1.24-alpine AS builder

# Permite que o Go resolva as versões automaticamente
ENV GOTOOLCHAIN=auto
ENV GO111MODULE=on 

WORKDIR /app

# 1. Copia TUDO primeiro (incluindo o código e os mods)
COPY . .

# 2. Limpa e organiza as dependências com o código já presente
RUN go mod tidy

# 3. Compila o binário
RUN go build -o xml-service .

# Estágio final
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/xml-service .
COPY --from=builder /app/.env .

EXPOSE 8080 50051

CMD ["./xml-service"]