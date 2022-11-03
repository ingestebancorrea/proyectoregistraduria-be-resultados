from Repositorios.InterfaceRepositorio import InterfaceRepositorio
from Modelos.Partido import Partido

from bson import ObjectId
class RepositorioPartido(InterfaceRepositorio[Partido]):
    def getListadoPartido(self, id_partido):
        theQuery = {"partido.$id": ObjectId(id_partido)}
        return self.query(theQuery)