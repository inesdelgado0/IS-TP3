import graphene
import grpc
import comunicacao_pb2
import comunicacao_pb2_grpc
from flask import Flask
from flask_graphql import GraphQLView

class MarcaStatsType(graphene.ObjectType):
    total = graphene.Int()
    media_preco = graphene.Float()
    media_kms = graphene.Float()

class LocalizacaoType(graphene.ObjectType):
    total_carros = graphene.Int()
    valor_total = graphene.Float()

class Query(graphene.ObjectType):
    # Queries disponíveis no GraphQL
    stock_marca = graphene.Field(MarcaStatsType, marca=graphene.String())
    count_segmento = graphene.Int(segmento=graphene.String())
    stats_localizacao = graphene.Field(LocalizacaoType, cidade=graphene.String())

    def resolve_stock_marca(self, info, marca):
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = comunicacao_pb2_grpc.BIQueryServiceStub(channel)
            res = stub.GetMarcaStats(comunicacao_pb2.Filtro(termo=marca))
            return MarcaStatsType(total=res.total, media_preco=res.media_preco, media_kms=res.media_kms)

    def resolve_count_segmento(self, info, segmento):
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = comunicacao_pb2_grpc.BIQueryServiceStub(channel)
            res = stub.GetContagemSegmento(comunicacao_pb2.Filtro(termo=segmento))
            return int(res.valor)

    def resolve_stats_localizacao(self, info, cidade):
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = comunicacao_pb2_grpc.BIQueryServiceStub(channel)
            res = stub.GetLocalizacaoStats(comunicacao_pb2.Filtro(termo=cidade))
            return LocalizacaoType(total_carros=res.total_carros, valor_total=res.valor_total)

# 1. Definir o Schema (garante que o nome é consistente)
schema = graphene.Schema(query=Query)

# 2. Criar a App Flask
app = Flask(__name__)

# 3. Configurar a Rota
app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view(
        'graphql',
        schema=schema, # <--- Agora o nome bate certo
        graphiql=True
    )
)

if __name__ == '__main__':
    print("------------------------------------------")
    print("BI Service (Python/GraphQL) ON")
    print("URL: http://localhost:5000/graphql")
    print("------------------------------------------")
    app.run(port=5000)