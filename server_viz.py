    
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

def histograma_ods(df):
    li=list()
    actual = df.groupby(['ods'],as_index=False).agg({"idPregunta": "count"})
    
    for index, row in actual.iterrows():
        li.append({'count':row['idPregunta'],'text':row['ods']})
    
    return {'items':li}


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


@app.route('/',methods=['POST'])
def hello_world():
    return jsonify(request.json)


@app.route('/odsComuna',methods=['POST'])
def ods_comuna():

		query = request.json
		df_fil=df[(df.rangoEdad.isin(query['edades'])) & (df.sexo.isin(query['sexos']))]

		return Response(json.dumps(comuna_vs_ods(df_fil,query['numero'])),mimetype='application/json')

@app.route('/histograma_ods',methods=['POST'])
def histograma():

		query = request.json
		df_fil=df[(df.rangoEdad.isin(query['edades'])) & (df.sexo.isin(query['sexos']))]

		return Response(json.dumps(histograma_ods(df_fil)),mimetype='application/json')


@app.route('/sunburst',methods=['POST'])
def sunburst_r():

		query = request.json
		df_fil=df[(df.rangoEdad.isin(query['edades'])) & (df.sexo.isin(query['sexos']))]

		return Response(json.dumps(sunburst(df_fil,query['numero'])),mimetype='application/json')



def pa(path):
	current = os.path.dirname(os.path.abspath(__file__))
	return os.path.join(current, path)




df=None
if __name__ == "__main__":
	df = pd.read_pickle('datos_filtrados.pkl')
	app.run(debug=True, port=5000, host='0.0.0.0')
