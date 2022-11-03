from Repositorios.InterfaceRepositorio import InterfaceRepositorio
from Modelos.Resultado import Resultado
from bson import ObjectId

class RepositorioResultado(InterfaceRepositorio[Resultado]):
    
    def getListadoResultadosEnCandidato(self,id_candidato):
        theQuery = {"candidato.$id": ObjectId(id_candidato)}
        return self.query(theQuery)

    def sumaVotosPorCandidato(self, id_candidato):
        query1 = {
            "$match" : {"candidato.$id": ObjectId(id_candidato)}
        }
        query2 = {
            "$group":{
                "_id":"$candidato",
                "Suma_votos": {
                    "$sum":"$numero_votos"
                }
            }
        }
        pipeline = [query1, query2]
        return self.queryAggregation(pipeline)

    def totalVotosPorCandidato(self):
        laColeccion = self.baseDatos[self.coleccion]
        myQuery =[
            {
                '$group': {'_id': '$candidato','Total votos': {'$sum': '$numero_votos'}}
            }, {
                '$sort': {'Total votos': -1}
            }, {
                '$lookup': {'from': 'candidato','localField': '_id.$id','foreignField': '_id','as': 'Nombre'}
            }, {
                '$lookup': {'from': 'partido','localField': 'Nombre.partido.$id','foreignField': '_id','as': 'Partido'}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Partido', 0]}, '$$ROOT']}}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Nombre', 0]}, '$$ROOT']}}
            }, {
                '$project': {'Nombre': {'$concat': ['$nombre', ' ', '$apellido']},'Partido': "$partido",'Total votos': 1}
            }
        ]
        data = []
        for x in laColeccion.aggregate(myQuery):
            x["_id"] = x["_id"].__str__()
            x = self.transformObjectIds(x)
            x = self.getValuesDBRef(x)
            data.append(x)
        return data

    def totalVotosCandidatoPorMesa(self,id_mesa):
        laColeccion = self.baseDatos[self.coleccion]
        myQuery = [
            {
                '$match': {'mesa.$id': ObjectId(id_mesa)}
            }, {
                '$sort': {'numero_votos': -1}
            }, {
                '$lookup': {'from': 'candidato','localField': 'candidato.$id','foreignField': '_id','as': 'Nombre'}
            }, {
                '$lookup': {'from': 'partido','localField': 'Nombre.partido.$id','foreignField': '_id','as': 'Partido'}
            }, {
                '$lookup': {'from': 'mesa','localField': 'mesa.$id','foreignField': '_id','as': 'Mesa'}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Partido', 0]}, '$$ROOT']}}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Nombre', 0]}, '$$ROOT']}}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Mesa', 0]}, '$$ROOT']}}
            }, {
                '$project': {'Nombre': {'$concat': ['$nombre', ' ', '$apellido']},'Partido': '$partido',
                             'Mesa': '$numero','Total votos': '$numero_votos'}
            }
        ]
        data = []
        for x in laColeccion.aggregate(myQuery):
            x["_id"] = x["_id"].__str__()
            x = self.transformObjectIds(x)
            x = self.getValuesDBRef(x)
            data.append(x)
        return data

    def totalVotosPorMesa(self):
        laColeccion = self.baseDatos[self.coleccion]
        myQuery =[
            {
                '$group': {'_id': '$mesa','Total votos': {'$sum': '$numero_votos'}}
            }, {
                '$sort': {'Total votos': 1}
            }, {
                '$lookup': {'from': 'mesa','localField': '_id.$id','foreignField': '_id','as': 'Mesa'}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Mesa', 0]}, '$$ROOT']}}
            }, {
                '$project': {'Mesa': '$numero','Total votos': 1}
            }
        ]
        data = []
        for x in laColeccion.aggregate(myQuery):
            x["_id"] = x["_id"].__str__()
            x = self.transformObjectIds(x)
            x = self.getValuesDBRef(x)
            data.append(x)
        return data

    def totalVotosPorPartido(self):
        laColeccion = self.baseDatos[self.coleccion]
        myQuery =[
            {
                '$lookup': {'from': 'candidato','localField': 'candidato.$id','foreignField': '_id','as': 'Nombre'}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Nombre', 0]}, '$$ROOT']}}
            }, {
                '$group': {'_id': '$partido','Total votos': {'$sum': '$numero_votos'}}
            }, {
                '$lookup': {'from': 'partido','localField': '_id.$id','foreignField': '_id','as': 'Partido'}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Partido', 0]}, '$$ROOT']}}
            }, {
                '$project': {'Partido': '$partido','Total votos': 1}
            }, {
                '$sort': {'Total votos': -1}
            }
        ]
        data = []
        for x in laColeccion.aggregate(myQuery):
            x["_id"] = x["_id"].__str__()
            x = self.transformObjectIds(x)
            x = self.getValuesDBRef(x)
            data.append(x)
        return data

    def totalVotosPartidoPorMesa(self,id_mesa):
        laColeccion = self.baseDatos[self.coleccion]
        myQuery = [
            {
                '$match': {'mesa.$id': ObjectId(id_mesa)}
            }, {
                '$lookup': {'from': 'candidato','localField': 'candidato.$id','foreignField': '_id','as': 'Nombre'}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Nombre', 0]}, '$$ROOT']}}
            }, {
                '$group': {'_id': '$partido','Total votos': {'$sum': '$numero_votos'}}
            }, {
                '$sort': {'Total votos': -1}
            }, {
                '$lookup': {'from': 'partido','localField': '_id.$id','foreignField': '_id','as': 'Partido'}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Partido', 0]}, '$$ROOT']}}
            }, {
                '$project': {'Partido': '$partido','Total votos': 1}
            }
        ]
        data = []
        for x in laColeccion.aggregate(myQuery):
            x["_id"] = x["_id"].__str__()
            x = self.transformObjectIds(x)
            x = self.getValuesDBRef(x)
            data.append(x)
        return data
    
        def distribucionPorcentual(self):
        laColeccion = self.baseDatos[self.coleccion]
        myQuery =[
            {
                '$lookup': {'from': 'candidato','localField': 'candidato.$id','foreignField': '_id','as': 'Nombre'}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Nombre', 0]}, '$$ROOT']}}
            }, {
                '$group': {'_id': {'partido': '$partido'},'count': {'$sum': 1}}
            }, {
                '$project':
                    {'count': 1,'percentage':
                        {'$concat': [{'$substr': [{'$multiply': [{'$divide': ['$count', 15]}, 100]}, 0, 4]}, '', '%']}}
            }, {
                '$project': {'percentage': 1,'Partido': {'$objectToArray': '$_id'}}
            }, {
                '$lookup': {'from': 'partido','localField': 'Partido.v.$id','foreignField': '_id','as': 'Partido'}
            }, {
                '$replaceRoot': {'newRoot': {'$mergeObjects': [{'$arrayElemAt': ['$Partido', 0]}, '$$ROOT']}}
            }, {
                '$project': {'Porcentaje': '$percentage','Partido': '$partido'}
            }
        ]
        data = []
        for x in laColeccion.aggregate(myQuery):
            x["_id"] = x["_id"].__str__()
            x = self.transformObjectIds(x)
            x = self.getValuesDBRef(x)
            data.append(x)
        return data
