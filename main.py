from fastapi import FastAPI
from fastapi import Query
import pandas as pd
import datetime

app = FastAPI()


@app.get("/")
async def root():
    return {"Mi primera Api, Laura Maita"}

@app.get("/get_max_duration/")
def get_max_duration(year:int=Query(None),platform:str=Query(None),duration_type:str=Query(None)):
    #Película con mayor duración con filtros opcionales de AÑO, PLATAFORMA Y TIPO DE DURACIÓN
    
    if platform == 'netflix':
        inicioId = 'n'
    elif platform == 'hulu':
        inicioId = 'h'
    elif platform == 'disney':
        inicioId = 'd'
    elif platform == 'amazon':
        inicioId = 'a'
    elif platform is None:
        inicioId = None
    else:
        return 'no se encuentra plataforma, ingrear una de las opciones: netflix, hulu, disney o amazon'
    

    if duration_type == 'season':
        durationtype= 'season'
    elif duration_type == 'film':
        durationtype = 'min'
    else:
        return 'ingresar season o film en minuscula'

    df_plataformagral =  pd.read_csv('totalplataformas.csv', sep=',') 
    

    if year and platform and  duration_type  is not None:

        filtro1= df_plataformagral[df_plataformagral.id.str.contains(inicioId) #filtro por primer letra de plataforma
                            & (df_plataformagral.release_year==year)  #filtro por el año
                            & (df_plataformagral.duration_type==durationtype) ] #filtro por la duracion
        max = filtro1['duration_int'].max()
        filtro2 = filtro1.query("duration_int==@max")
        pelicula = filtro2[['title', 'duration_int', 'duration_type']]

        if  not pelicula.empty:

            return pelicula
    
    elif year is None:
        
        if inicioId is None :
            
            filtro1= df_plataformagral[(df_plataformagral.duration_type==durationtype) ] #filtro por la duracion
            max = filtro1['duration_int'].max()
            filtro2 = filtro1.query("duration_int==@max")
            pelicula = filtro2[['title', 'duration_int', 'duration_type']]
            return pelicula         
        
        filtro1= df_plataformagral[df_plataformagral['id'].str.contains(inicioId) #filtro por primer letra de plataforma
                            & (df_plataformagral.duration_type==durationtype) ] #filtro por la duracion
        max = filtro1['duration_int'].max()
        filtro2 = filtro1.query("duration_int==@max")
        pelicula = filtro2[['title', 'duration_int', 'duration_type']]
        return pelicula
                   
    elif inicioId is None :
        if durationtype is None:

            filtro1= df_plataformagral[(df_plataformagral.release_year==year)]#filtro por año                    
            max = filtro1['duration_int'].max()
            filtro2 = filtro1.query("duration_int==@max")
            pelicula = filtro2[['title', 'duration_int', 'duration_type']]
            return pelicula    
     
        filtro1= df_plataformagral[(df_plataformagral.release_year==year) #filtro por año
                                   & (df_plataformagral.duration_type==durationtype) ] #filtro por la duracion
        max = filtro1['duration_int'].max()
        filtro2 = filtro1.query("duration_int==@max")
        pelicula = filtro2[['title', 'duration_int', 'duration_type']]   
        return pelicula 

@app.get("/get_score_count/")
def get_score_count(platform:str, scored:float, year:int): #Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año 

    if platform == 'netflix':
        plataforma = 'n'
    elif platform == 'hulu':
        plataforma = 'h'
    elif platform == 'disney':
        plataforma = 'd'
    elif platform == 'amazon':
        plataforma = 'a'
    else:
        return 'ingresar una de las opciones: netflix, hulu, amazon o disney'
    

    df_ratings = pd.read_csv(r'transformacion_ratings1.csv', sep=',')
    df_ratings['fecha']= pd.to_datetime(df_ratings['fecha']) #modifico el tipo de objeto
    df_ratings['year']= df_ratings['fecha'].dt.year #agrego columna de año
    consulta_ratings= df_ratings[(df_ratings.year==year)
                            & (df_ratings.id.str.contains(plataforma))] #filtro por id

    consulta_score = consulta_ratings[consulta_ratings.score> scored ]

    if consulta_score.empty :
        return 'ingresar scored mayor a 3.3 y menor a 3.8, o el año ingresado no tiene registro'
    else:
       return {'cantidad de peliculas':int(consulta_score.id.count())}

@app.get("/get_count_platform/")   
def get_count_platform(platform:str): #Cantidad de películas por plataforma con filtro de PLATAFORMA

    if platform == 'netflix':
        plataforma = 'n'
    elif platform == 'hulu':
        plataforma= 'h'
    elif platform == 'disney':
        plataforma= 'd'
    elif platform == 'amazon':
        plataforma= 'a'
    else:
        return 'ingrese plataforma: amazon, disney, hulu o netflix'

    df_plataformagral =  pd.read_csv('totalplataformas.csv', sep=',')
    consulta_plataforma= df_plataformagral[(df_plataformagral.id.str.contains(plataforma))
                                           & (df_plataformagral.duration_type=='min')]
    conteo_movies= consulta_plataforma.title.count()
    return int(conteo_movies)

@app.get("/get_actor/") 
def get_actor(platform:str, year:int): #Actor que más se repite según plataforma y año

    if platform == 'netflix':
        plataforma= 'n'
    elif platform == 'disney':
        plataforma= 'd'
    elif platform == 'hulu':
        plataforma= 'h'
    elif platform == 'amazon':
        plataforma= 'a'
    else:
        return 'ingresar plataforma valida: amazon, disney, hulu o netflix'

    
    df_plataformagral =  pd.read_csv('totalplataformas.csv', sep=',')   

    filtroplat = df_plataformagral[(df_plataformagral.id.str.contains(plataforma))
                                   & (df_plataformagral.release_year==year)] #filtro por año y plataforma

    if filtroplat.empty:
        
        return 'el año no se encuentra en el dataset'
    else:
        lista= filtroplat['cast'] #guardo en nueva variable los actores por peliculas
        newlista = [x for x in lista if pd.isnull(x) == False]#elimino flotantes

        lista_cast=[]
        for i in newlista: #itero para separar actores en diferentes str
            separador= i.split(sep=',')
            for j in separador: #itero para eliminar espacios en cada nuevo str y agregar a lista
                    valores=j.strip()
                    lista_cast.append(valores) #agrego valores a lista_cast
        df_lista_cast= pd.Series(lista_cast) #transformo a dataframe
        return {'El actor mas repetido es': df_lista_cast.value_counts().idxmax()} #imprimo copnsulta del str mas repetido
