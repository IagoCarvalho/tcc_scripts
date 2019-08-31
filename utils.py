import pandas as pd

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['dados_de_transito']


def dataframe_to_mongo(df, collection):
    
    records = df.to_dict('records')
    db[collection].insert_many(records)

def mongo_to_dataframe(collection):

    cursor = db[collection].find() 
    df = pd.DataFrame(list(cursor))
    del df['_id']
    
    return df

def clear_database():
    db.drop_collection("tipos_infracoes_2018")
    db.drop_collection("acidentes_fatais_2008_2017")
    db.drop_collecttion("pedestres_mortos_trechos_ns_2008_2017")
    db.drop_collection("cursos_oferecidos_detran_2018")
    db.drop_collection("campanhas_educativas_detran_2012_2019")
    db.drio_collection("acidentes_rodovias_2017_2019")