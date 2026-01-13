import os  # Importa isto para ler variáveis de ambiente
import graphene
import grpc
import comunicacao_pb2
import comunicacao_pb2_grpc
from flask import Flask
from flask_graphql import GraphQLView

# Lógica para detetar se estamos no Docker ou no PC local
# Se a variável GRPC_HOST não existir, assume 'localhost'
GRPC_HOST = os.getenv('GRPC_HOST', 'localhost')
GRPC_ADDR = f"{GRPC_HOST}:50051"

class MarcaStatsType(graphene.ObjectType):
    total = graphene.Int()
    media_preco = graphene.Float()
    media_kms = graphene.Float()

class LocalizacaoType(graphene.ObjectType):
    total_carros = graphene.Int()
    valor_total = graphene.Float()

class Query(graphene.ObjectType):
    stock_marca = graphene.Field(MarcaStatsType, marca=graphene.String())
    count_segmento = graphene.Int(segmento=graphene.String())
    stats_localizacao = graphene.Field(LocalizacaoType, cidade=graphene.String())

    def resolve_stock_marca(self, info, marca):
        # Usamos GRPC_ADDR em vez de 'localhost:50051'
        with grpc.insecure_channel(GRPC_ADDR) as channel:
            stub = comunicacao_pb2_grpc.BIQueryServiceStub(channel)
            res = stub.GetMarcaStats(comunicacao_pb2.Filtro(termo=marca))
            return MarcaStatsType(total=res.total, media_preco=res.media_preco, media_kms=res.media_kms)

    def resolve_count_segmento(self, info, segmento):
        with grpc.insecure_channel(GRPC_ADDR) as channel:
            stub = comunicacao_pb2_grpc.BIQueryServiceStub(channel)
            res = stub.GetContagemSegmento(comunicacao_pb2.Filtro(termo=segmento))
            return int(res.valor)

    def resolve_stats_localizacao(self, info, cidade):
        with grpc.insecure_channel(GRPC_ADDR) as channel:
            stub = comunicacao_pb2_grpc.BIQueryServiceStub(channel)
            res = stub.GetLocalizacaoStats(comunicacao_pb2.Filtro(termo=cidade))
            return LocalizacaoType(total_carros=res.total_carros, valor_total=res.valor_total)

schema = graphene.Schema(query=Query)
app = Flask(__name__)

app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True)
)

if __name__ == '__main__':
    # No Docker, o Flask precisa de correr em 0.0.0.0 para ser visível fora do contentor
    app.run(host='0.0.0.0', port=5000)