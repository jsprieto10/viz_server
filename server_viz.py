    
from flask import Flask,request,Response,jsonify
import json
import os
import pandas as pd
import sys

app = Flask(__name__)


def comuna_vs_ods(df,numero_ods=3):
    li=list()
    groupped = df.groupby(['comuna','ods'],as_index=False).agg({"idPregunta": "count"})
    comunas = groupped.comuna.unique()
    for comuna in comunas:
        df_actual=groupped[groupped['comuna']==comuna]

        info_comuna = {'id': "C"+comuna.split(')')[0],'comuna':comuna,'datos':[]}

        for index, row in df_actual.iterrows():
            info_comuna['datos'].append({'name':row['ods'], 'value':row['idPregunta']})
            
        data = sorted(info_comuna['datos'],key=lambda x: x['value'],reverse=True)
        
        info_comuna['datos']=data[:min(max(1,numero_ods),len(data))]

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
            
    return 

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




@app.route('/answers/<ods>/<n>',methods=['GET'])
def answer_end(ods,n):
    print(ods,n)
    

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
		df_fil=df[(df.rangoEdad.isin(query['edades'])) & (df.sexo.isin(query['sexos']))]

		return Response(json.dumps(comuna_vs_ods(df_fil,query['numero'])),mimetype='application/json')


@app.route('/todos_comunas_ods',methods=['POST'])
def todos_comuna_ods():

        query = request.json
        df_fil=df[(df.rangoEdad.isin(query['edades'])) & (df.sexo.isin(query['sexos']))]

        return Response(json.dumps(todos_comuna(df_fil)),mimetype='application/json')


@app.route('/histograma_ods',methods=['POST'])
def histograma():

		query = request.json
		df_fil=df[(df.rangoEdad.isin(query['edades'])) & (df.sexo.isin(query['sexos']))]

		return Response(json.dumps(histograma_ods(df_fil,query['numero'])),mimetype='application/json')


@app.route('/sunburst',methods=['POST'])
def sunburst_r():

		query = request.json
		df_fil=df[(df.rangoEdad.isin(query['edades'])) & (df.sexo.isin(query['sexos']))]

		return Response(json.dumps(sunburst(df_fil,query['numero'])),mimetype='application/json')



def pa(path):
	current = os.path.dirname(os.path.abspath(__file__))
	return os.path.join(current, path)




df = pd.read_pickle('datos_filtrados.pkl')
if __name__ == "__main__":
	app.run(debug=True, port=8000, host='0.0.0.0')