    
from flask import Flask,request,Response,jsonify
import json
import os
import pandas as pd
import sys
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


def comuna_vs_ods(df,numero_ods=3):
    li=list()
    groupped = df.groupby(['comuna','ods'],as_index=False).agg({"idPregunta": "count"})
    metas = df.groupby(['comuna','ods','meta'],as_index=False).agg({"idPregunta": "count"})
    comunas = groupped.comuna.unique()
    for comuna in comunas:
        
        df_actual=groupped[groupped['comuna']==comuna]
        
        total_ods = df_actual['idPregunta'].sum()
        
        info_comuna = {'id': "C"+comuna.split(')')[0],'comuna':comuna,'datos':[]}

        for index, row in df_actual.iterrows():
            
            fil=metas[(metas.comuna == comuna) & (metas.ods==row['ods'])]
            total_meta = fil.idPregunta.sum()
            meta,cuenta=fil.loc[fil['idPregunta'].idxmax()][-2:]
            
            d = {'name':row['ods'],
                 'value':row['idPregunta'],
                 'porcentaje':row['idPregunta']/total_ods,
                 'meta':{
                     'name':meta,
                     'value':int(cuenta),
                     'porcentaje':cuenta/total_meta
                 }
                }
            
            info_comuna['datos'].append(d)
            
        data = sorted(info_comuna['datos'],key=lambda x: x['value'],reverse=True)
        
        info_comuna['datos']=data[:min(numero_ods,len(data))]

        li.append(info_comuna)
        
    return li

def histograma_ods(df,numero_ods=20):
    li=list()
    actual = df.groupby(['ods'],as_index=False).agg({"idPregunta": "count"})
    
    for index, row in actual.iterrows():
        li.append({'count':row['idPregunta'],'text':row['ods']})
    
    data = sorted(li,key=lambda x: x['count'],reverse=True)
    return {'items':data[:min(max(1,numero_ods),len(data))]}


def sunburst(df,numero_ods=40):
    li=list()
    groupped = df.groupby(['ods','meta'],as_index=False).agg({"idPregunta": "count"})
    odss = groupped.ods.unique()
    for ods in odss:
        df_actual=groupped[groupped['ods']==ods]

        info_comuna = {'name':ods,'children':[]}

        for index, row in df_actual.iterrows():
            info_comuna['children'].append({'name':row['meta'], 'value':row['idPregunta']})
            
        data = sorted(info_comuna['children'],key=lambda x: x['value'],reverse=True)
        
        info_comuna['children']=data[:min(numero_ods,len(data))]

        li.append(info_comuna)
        
    return {"name":"ODS's", "children":li}

def todos_comuna(df,numero_ods=20):
    li=list()
    groupped = df.groupby(['comuna','ods'],as_index=False).agg({"idPregunta": "count"})
    comunas = groupped.comuna.unique()
    for comuna in comunas:
        df_actual=groupped[groupped['comuna']==comuna]

        info_comuna = {'id': "C"+comuna.split(')')[0],'comuna':comuna,'datos':{}}

        for index, row in df_actual.iterrows():
            info_comuna['datos'][row['ods']]=row['idPregunta']
                

        li.append(info_comuna)
            
    return li

def sexos(df, ods):
    groupped = df[df.ods==ods].groupby(['ods','sexo'],as_index=False).agg({"idPregunta": "count"})
    return {"Hombres": int(groupped[groupped.sexo=='Hombre'].iloc[0,2]), "Mujeres": int(groupped[groupped.sexo=='Mujer'].iloc[0,2])}



def hexa_mapa(df,n=3000):
    
    res = []
    
    sample = df.sample(n)
    
    for index, row in sample.iterrows():
        
        res.append({'comuna':row['comuna'], 'ods': int(row['ods'].split('_')[-1])})
        
    return res


def answers(df, ods, n):

    fil=list(df[df.ods==ods].sample(n)['respuesta'])
    
    return [{'respuesta': answer} for answer in fil]


def filtrado(df2,query):
	ods = query['ods'] if 'ods' in query else df2.ods.unique()
	comunas = query['comunas'] if 'comunas' in query  else df2.comuna.unique()
	metas = query['metas'] if 'metas' in query  else df2.meta.unique()

	df = df2[(df2.ods.isin(ods)) & (df2.comuna.isin(comunas)) & (df2.meta.isin(metas))]

	lista={
	'jovenes':['0 a 9','10 a 14', '15 a 19','20 a 24','25 a 29'],
	'adultos':['30 a 34', '35 a 39', '40 a 44', '45 a 49', '50 a 54'],
	'mayores':['55 a 59', '60 a 64', '65 a 69','70 a 74', '75 a 79', '80 o más']
	}
	edad = sum([lista[rango] for rango in query['edades']],[])
	return df[(df.rangoEdad.isin(edad)) & (df.sexo.isin(query['sexos'])) & (df.idPregunta.isin(query['respuesta']))]


@app.route('/answers/<ods>/<n>',methods=['GET'])
def answer_end(ods,n):
    return Response(json.dumps(answers(df,ods,int(n))),mimetype='application/json')



@app.route('/hexa/<n>', methods=['GET'])
def hexa_end(n):
    return Response(json.dumps(hexa_mapa(df,int(n))),mimetype='application/json')



@app.route('/',methods=['POST'])
def hello_world():
    return jsonify(request.json)

@app.route('/sexos/<ods>',methods=['GET'])
def sexos_path(ods):
        a=sexos(df,ods)
        print(a)
        return Response(json.dumps(a),mimetype='application/json')

@app.route('/odsComuna',methods=['POST'])
def ods_comuna():

		query = request.json
		df_fil=filtrado(df,query)

		return Response(json.dumps(comuna_vs_ods(df_fil,query['numero'])),mimetype='application/json')


@app.route('/todos_comunas_ods',methods=['POST'])
def todos_comuna_ods():

        query = request.json
        df_fil=filtrado(df,query)
        print(query)
        return Response(json.dumps(todos_comuna(df_fil)),mimetype='application/json')


@app.route('/histograma_ods',methods=['POST'])
def histograma():
    query = request.json
    print("hola",query)
    df_fil=filtrado(df,query)
    
    return Response(json.dumps(histograma_ods(df_fil,query['numero'])),mimetype='application/json')


@app.route('/sunburst',methods=['POST'])
def sunburst_r():
    query = request.json
    df_fil=filtrado(df,query)
    return Response(json.dumps(sunburst(df_fil,query['numero'])),mimetype='application/json')

@app.route('/historias/<n>', methods=['POST'])
def stories(n):

	query = request.json
	df_fil=filtrado(df,query)[df.respuesta.notnull()]
	res = []

	sample = df_fil.sample(int(n))
	d={'ods_1': 'Fin de la pobreza',
        'ods_2': 'Hambre cero',
        'ods_3': 'Salud y bienestar',
        'ods_4': 'Educación de calidad',
        'ods_5': 'Igualdad de género',
        'ods_6': 'Agua limpia y saneamiento',
        'ods_7': 'Energía asequible y no contaminante',
        'ods_8': 'Trabajo y crecimiento económico',
        'ods_9': 'Industria, innovación e infraestructura',
        'ods_10': 'Reducción de las desigualdades',
        'ods_11': 'Ciudades y comunidades sostenible',
        'ods_12': 'Producción y consumo responsables',
        'ods_13': 'Acción por el clima',
        'ods_14': 'Vida submarina',
        'ods_15': 'Vida de ecosistemas terrestres',
        'ods_16': 'Paz, justicia e instituciones sólidas',
        'ods_17': 'Alianzas para lograr los objetivos'}

	for index, row in sample.iterrows():
		persona={}
		persona['sexo']=row['sexo']
		persona['rangoEdad']=row['rangoEdad']
		persona['comuna']=row['comuna']
		persona['barrio']=row['barrio']
		persona['pregunta']=row['pregunta']
		persona['respuesta']=row['respuesta']
		persona['fecha']=str(row['fechaCorta'])
		persona['ods']=str(row['ods'])+": "+d[row['ods']]
		persona['meta']=str(row['meta'])
		res.append(persona)

	return Response(json.dumps(res),mimetype='application/json')


@app.route('/porcentaje',methods=['POST'])
def porcentaje():

    query = request.json
    df_fil=filtrado(df,query)

    porcentaje=100*len(df_fil)/len(df)
    return jsonify({'porcentaje':round(porcentaje,2 )})



def pa(path):
	current = os.path.dirname(os.path.abspath(__file__))
	return os.path.join(current, path)



df = pd.read_pickle('datos_filtrados.pkl')
if __name__ == "__main__":
	app.run(debug=True, port=8000, host='0.0.0.0')