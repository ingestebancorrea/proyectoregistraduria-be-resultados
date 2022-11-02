from Modelos.Resultado import Resultado
from Modelos.Mesa import Mesa
from Modelos.Candidato import Candidato
from Repositorios.RepositorioResultado import RepositorioResultado
from Repositorios.RepositorioMesa import RepositorioMesa


from Repositorios.RepositorioCandidato import RepositorioCandidato
class ControladorResultado():
    def __init__(self):
        self.repositorioResultado = RepositorioResultado()
        self.repositorioMesa = RepositorioMesa()
        self.repositorioCandidato = RepositorioCandidato()
    def index(self):
        return self.repositorioResultado.findAll()
    """
    Asignacion mesa y candidato a resultado
    """
    def create(self,infoResultado,id_mesa,id_candidato):
        nuevoResultado=Resultado(infoResultado)
        laMesa=Mesa(self.repositorioMesa.findById(id_mesa))
        elCandidato=Candidato(self.repositorioCandidato.findById(id_candidato))
        nuevoResultado.mesa=laMesa
        nuevoResultado.candidato=elCandidato
        return self.repositorioResultado.save(nuevoResultado)
    def show(self,id):
        elResultado=Resultado(self.repositorioResultado.findById(id))
        return elResultado.__dict__
    """
    Modificación de resultados (mesa y candidato)
    """
    def update(self,id,infoResultado,id_mesa,id_candidato):
        elResultado=Resultado(self.repositorioResultado.findById(id))
        elResultado.numero_votos=infoResultado["numero_votos"]
        elMesa = Mesa(self.repositorioMesa.findById(id_mesa))
        elCandidato = Candidato(self.repositorioCandidato.findById(id_candidato))
        elResultado.mesa = laMesa
        elResultado.candidato = elCandidato
        return self.repositorioResultado.save(elResultado)
    def delete(self, id):
        return self.repositorioResultado.delete(id)
    
    #Metodos para visualizar reportes

    def listarResultadosEnCandidato(self,id_candidato):
        return self.repositorioResultado.getListadoResultadosEnCandidato(id_candidato)

    def votosMasAltosPorMesa(self):
        return self.repositorioResultado.getMayorVotacionPorMesa()

    def sumaVotosPorCandidato(self,id_candidato):
        return self.repositorioResultado.sumaVotosPorCandidato(id_candidato)

    def totalVotosPorCandidato(self):
        return self.repositorioResultado.totalVotosPorCandidato()

    def totalVotosCandidatoPorMesa(self,id_mesa):
        return self.repositorioResultado.totalVotosCandidatoPorMesa(id_mesa)

    def totalVotosPorMesa(self):
        return self.repositorioResultado.totalVotosPorMesa()

    def totalVotosPorPartido(self):
        return self.repositorioResultado.totalVotosPorPartido()

    def totalVotosPartidoPorMesa(self,id_mesa):
        return self.repositorioResultado.totalVotosPartidoPorMesa(id_mesa)
