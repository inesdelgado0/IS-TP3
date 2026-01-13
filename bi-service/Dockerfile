FROM python:3.10-slim

WORKDIR /app

# Copia a lista de bibliotecas e instala-as
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o resto (app.py e os ficheiros pb2)
COPY . .

# Expõe a porta do Flask/GraphQL
EXPOSE 5000

CMD ["python", "app.py"]