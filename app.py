# main.py
#Importación de las librerias necesarias para el funcionamiento del código

from flask import Flask, request
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import timedelta
from datetime import datetime
import atexit
import psycopg2
import os
from dotenv import load_dotenv
import copy
from apscheduler.triggers.cron import CronTrigger
import logging



#Encabezado de flask 
load_dotenv()


'''   importar os - para guardar attachments
        establecer ruta de la carpeta y guardarla en un campo de la tabla
        '''


app = Flask(__name__)

logging.basicConfig(filename='record.log', level=logging.INFO, format=f'%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s')
 
 #Ruta por defecto para testear la API corriendo en un puerto 
@app.route('/basic_api/hello_world')
def hello_world():
    return 'Hello, World!'


if __name__ == '__main__':
    app.run(debug=True)



# Conexión con la base de datos 
#verificacion en consola del entorno al cual se esta conectado

print(os.getenv("dbhost"))
#Conexión con la base de datos del entorno establecido en el archivo .env con las credenciales y rutas de las bases de datos y features
def get_db_connection():
    conn = psycopg2.connect(host=os.getenv("dbhost"),
                            database=os.getenv("database"),
                            user=os.getenv("dbuser"),
                            password=os.getenv("dbpassword"))
    return conn


RegularDate = datetime.today() - timedelta(hours=3, minutes=0)
RegularDate = RegularDate.strftime("%m/%d/%Y")

#Asignación de las variables del entorno (Inicializadas en el archivo .env)
tokenUrl = os.getenv("tokenUrl")
featureTerrenoQueryUrl = os.getenv("featureTerrenoQueryUrl")
featureOfertasQuertyUrl = os.getenv("featureOfertasQuertyUrl")
featureConstruccionesQueryUrl = os.getenv("featureConstruccionesQueryUrl")
featureUnidadConstruccionQueryUrl = os.getenv(
    "featureUnidadConstruccionQueryUrl")
featureOfertasInmobiliarias = os.getenv("featureOfertasInmobiliarias")
#Construccion del payLoad o request a la api de esri para la generación del token
tokenPayload = {
    "username":  os.getenv("portalUsername"),
    "password": os.getenv("portalpassword"),
    "client": "referer",
    "referer": 'featureQueryUrl',
    "expiration": 60,
    "f": "json"}
#Generación del PayLoad o request del metodo query a la api del servicio de los features 
terrenoQueryPayload = {"where": "1=1",
                           "geometryType": "esriGeometryEnvelope",
                           "spatialRel": "esriSpatialRelIntersects",
                           "units": "esriSRUnit_Meter",
                           "outFields": "*",
                           "outSR":'{     "wkid": 4326,     "latestWkid": 4326}',
                           "returnGeometry": True,
                           "returnIdsOnly": False,
                           "returnCountOnly": False,
                           "returnExtentOnly": False,
                           "returnDistinctValues": False,
                           "returnZ": False,
                           "returnM": False,
                           "sqlFormat": "none",
                           "f": "json",
                           "token": 'Token'}

#Funcion que retorna los features en alojados en el servidor de esri a partir de los urls presentes en .env

def featureFields(QuertyUrl,where='1=1'):
        ftokenPayload = copy.copy(tokenPayload)
        ftokenPayload['referer'] = QuertyUrl
        # print(ftokenPayload)
        ConatrucionReq = requests.post(
        tokenUrl, data=ftokenPayload, verify=False)
        resp = ConatrucionReq.json()
        # print(resp)
        ConstruccionQueryPayload = copy.copy(terrenoQueryPayload)
        ConstruccionQueryPayload['token'] = resp['token']
        ConstruccionQueryPayload['where'] = where
        FeaturesResQuery = requests.get(
        QuertyUrl, params=ConstruccionQueryPayload)
        #print( FeaturesResQuery.json())
        response_json = FeaturesResQuery.json()
        Features =  response_json['features'] if response_json['features'] else []
        return Features

#Funcion de actualización de las bases de datos 
def updateFieldsToDb():
    #funcion inicial para limpiar las tables y restablecer los contadores de los indentificadores automáticos
    def cleanTables():
        cur.execute('delete from act_tipologiaconstruccion')
        cur.execute('delete from act_linderos')
        cur.execute('delete from  act_objetoconstruccion')
        cur.execute('delete from  act_grupocalificacion')
        cur.execute('delete from  act_califconvencional')
        cur.execute('delete from  act_califnoconvencion')
        cur.execute('delete from act_unidadconstruccion')
        cur.execute('delete from act_construccion')
        cur.execute('delete from act_terreno')
        cur.execute('delete from act_ofertasmerinm')
        cur.execute('delete from act_contactovisita')
        cur.execute('delete from act_datoslevantamiento')
        cur.execute('ALTER SEQUENCE actterrenoid RESTART WITH 1;')
        cur.execute('ALTER SEQUENCE actconstruccionid RESTART WITH 1;')
        cur.execute('ALTER SEQUENCE actunidadcontruccionid RESTART WITH 2547;')
        cur.execute('ALTER SEQUENCE actdatoslevantamientoid RESTART WITH 1;')
        cur.execute('ALTER SEQUENCE actofertasid RESTART WITH 1;')
        cur.execute('ALTER SEQUENCE actlinderoid RESTART WITH 1;')
        cur.execute('ALTER SEQUENCE actcontactovisitaid  RESTART WITH 1;')
        
        conn.commit()
        app.logger.info(" se limpiaron todas las tablas. ")

    
        ''
    terrenoFeatures = featureFields(featureTerrenoQueryUrl,'procedimiento is not null')
    ConstruccionFeatures =featureFields(featureConstruccionesQueryUrl)
    UnidadConstruccionFeatures = featureFields(featureUnidadConstruccionQueryUrl)
    ofertaFeatures = featureFields(featureOfertasInmobiliarias)
    #Funcion (C) para obtener todos los códigos de construcción 
    def getCodigos(ar):
            return ar['attributes']['pk_constru']
        
    unidadCodigos=list(map(getCodigos,UnidadConstruccionFeatures))
    
    # print(unidadCodigos)


    conn = get_db_connection()
    cur = conn.cursor()
    control = []

    def impactDatosLevantamiento(ofertaFeatures):
        
        for i in ofertaFeatures:
            
            item= (i['attributes'])
            selectPredioId ="select actpredioid from public.act_predio where actpredionumpred = '{}'".format(item['codigo_terreno'])
            cur.execute(selectPredioId)
            try:
                predio_id=cur.fetchone()[0]
                
                # print(predio_id)
                acttipoofertaid = 2 if item['act_tipo_oferta'] == 1 else 1
                actvalornegociado = item['act_valor_negociado'] if item['act_valor_negociado']  != None else 0 
                actvalorpedido= item['act_valor_pedido'] if item['act_valor_pedido']  != None else 0
                acttiemofeenmer= item['act_tiempo_oferta']  if item['act_tiempo_oferta']  != None else 'No registra'
                actnumconofe= item['act_numero_contacto'] if item['act_numero_contacto']!= None else 'No registra'
                actnombreofe= item['act_nombre_oferente'] if  item['act_nombre_oferente'] != None else 'No registra'
                actofertafeccaptura=  datetime.fromtimestamp((item['act_fecha_captura_oferta'] )/1000)  if  item['act_fecha_captura_oferta'] != None else datetime.fromtimestamp((item['last_edited_date'] )/1000) 
                actofertasmerinmusucre = item['created_user'] if item['created_user'] != None else ''
                actofertasmerinmfechcre= datetime.fromtimestamp((item['created_date'] )/1000)
                actofertasmerinmipcre= item['created_user'] if item['created_user'] != None else ''
                actofertasmerinmusumod= item['last_edited_user'] if item['last_edited_user'] != None else ''
                actofertasmerinmfechmod= datetime.fromtimestamp((item['last_edited_date'] )/1000)
                actofertasmerinmipmod= item['created_user'] if item['created_user'] != None else ''
                actofertasmerinmusucre= item['last_edited_user'] if item['last_edited_user'] != None else ''
                
                insertOferta = "INSERT INTO public.act_ofertasmerinm (actpredioid,acttipoofertaid,actvalorpedido,actvalornegociado,actofertafeccaptura,acttiemofeenmer,actnumconofe,actnombreofe,actofertasmerinmipmod,actofertasmerinmfechmod,actofertasmerinmusumod,actofertasmerinmipcre,actofertasmerinmfechcre,actofertasmerinmusucre) VALUES ({},{},{},{},'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');".format(predio_id,acttipoofertaid,actvalorpedido,actvalornegociado,actofertafeccaptura,acttiemofeenmer,actnumconofe,actnombreofe,actofertasmerinmipmod,actofertasmerinmfechmod,actofertasmerinmusumod,actofertasmerinmipcre,actofertasmerinmfechcre,actofertasmerinmusucre)
                cur.execute(insertOferta)
                conn.commit()
            except:
                app.logger.warning("No se inserto en la tabla act_ofertasmerinm, el registro: {}".format(insertOferta))
                pass
            
            
    
    def Impacto_terrenos_construcciones(terrenoFeatures,ConstruccionFeatures,UnidadConstruccionFeatures):
        t_proc_notnnull=0
        t_actpredio = 0
        
        t_insert=0
        ut=0
        uc=0
        
        for i in (terrenoFeatures):
            t_proc_notnnull+=1


            npn = i['attributes']['codigo']
            actareaterreno= i['attributes']['act_area_r'] if i['attributes']['act_area_r'] != None and i['attributes']['act_area_r'] != "null" else 0
            actareadigitalterreno=i['attributes']['SHAPE__Area'] if i['attributes']['SHAPE__Area'] != None and i['attributes']['SHAPE__Area'] != "null" else 0
            #actavaluoterreno = i['attributes']['act_area_r'] if i['attributes']['act_area_r'] != None and i['attributes']['act_area_r'] != "null" else 0
            actley56 = True if i['attributes']['act_ley56'] == 1 else False
            acttipoviaid = i['attributes']['clase_via'] if i['attributes']['clase_via'] != None and i['attributes']['clase_via'] != "null" else 1
            actestadoviaid  = i['attributes']['estado_vias'] if i['attributes']['estado_vias'] != None and  i['attributes']['estado_vias'] >0 and i['attributes']['estado_vias'] != "null" else 1
            actinfluenciaviaid = i['attributes']['influencia_via'] if i['attributes']['influencia_via'] != None and i['attributes']['influencia_via'] != "null" and i['attributes']['influencia_via'] >0 else 1
            acttopografiaid = i['attributes']['codificacion'] if i['attributes']['codificacion'] != None and i['attributes']['codificacion'] != "null" else 1
            actdestinacionid = i['attributes']['act_destinaciones'] if i['attributes']['act_destinaciones'] != None and i['attributes']['act_destinaciones'] != "null" else 0
            actterrenoipmod = i['attributes']['last_edited_user'] if i['attributes']['last_edited_user'] != None and i['attributes']['last_edited_user'] != "null" else 1
            actterrenofechmod = datetime.fromtimestamp((i['attributes']['last_edited_date'])/1000)
            actterrenousumod = i['attributes']['last_edited_user'] if i['attributes']['last_edited_user'] != None and i['attributes']['last_edited_user'] != "null" else 1
            actterrenoipcre = i['attributes']['created_user'] if i['attributes']['created_user'] != None and i['attributes']['created_user'] != "null" else 1
            actterrenofechcre = datetime.fromtimestamp((i['attributes']['created_date'])/1000)
            actterrenousucre = i['attributes']['created_user'] if i['attributes']['created_user'] != None and i['attributes']['created_user'] != "null" else 1
            lindNorte =i['attributes']['lind'] if i['attributes']['lind']  != None else "No registra"
            lindSur=i['attributes']['act_lindsur'] if i['attributes']['act_lindsur']  != None else "No registra"
            lindOriente=i['attributes']['act_linor'] if i['attributes']['act_linor']  != None else "No registra"
            lindOccidente=i['attributes']['act_linocc'] if i['attributes']['act_linocc']  != None else "No registra"
            linderos = [lindNorte, lindSur, lindOriente, lindOccidente]
            acttienearearegistra	= True if i['attributes']['tiene_area_registral']== 1 else False
            actarearegistral	= i['attributes']['act_area_r'] if i['attributes']['act_area_r']!= None else 0
            actproccatastralregistralid	= 1 if i['attributes']['procedimiento']== 'Actualizacion' else 8
            actobservaciones	= i['attributes']['act_observ']  if i['attributes']['act_observ']!= None else 'No registra'
            actfechavisitapredial	= datetime.fromtimestamp((i['attributes']['act_fecha_'])/1000)  if i['attributes']['act_fecha_']!= None else datetime.fromtimestamp((i['attributes']['last_edited_date'])/1000)
            
            tipoidentificacion_id	= int(i['attributes']['tipo_de_documento_reconocedor']) if i['attributes']['tipo_de_documento_reconocedor']!= None else 1
            actnumdocreconocedor		= i['attributes']['numero_de'] if i['attributes']['numero_de']!= None else 'No registra'
            actprimernomreconocedor		= i['attributes']['segundo_nombre_reconocedor'] if i['attributes']['segundo_nombre_reconocedor']!= None else 'No registra'
            actsegnomreconocedor		= i['attributes']['segundo_nombre_del_reconocedor'] if i['attributes']['segundo_nombre_del_reconocedor']!= None else 'No registra'           
            actprimerapellrecon		= i['attributes']['primer_apellido_reconocedor'] if i['attributes']['primer_apellido_reconocedor']!= None else 'No registra'
            actsegapellrecon		= i['attributes']['segundo_apellido_reconocedor'] if i['attributes']['segundo_apellido_reconocedor']!= None else 'No registra'
            actresultadovisitaid		= i['attributes']['resultado_visita']+1 if  i['attributes']['resultado_visita']!= None and i['attributes']['resultado_visita']==0 else 8#i['attributes']['resultado_visita'] +1 if  i['attributes']['resultado_visita']!= None else 8
            actotroresultadovisita		= i['attributes']['act_otro_r'] if i['attributes']['act_otro_r']!= None else 'No registra'
            actsuscribeactacolin		= i['attributes']['acta_de_colindancia'] if i['attributes']['acta_de_colindancia']== True or i['attributes']['acta_de_colindancia']== False else False
            actdespojoabandono		= i['attributes']['despojo_o_abandono'] if  i['attributes']['acta_de_colindancia']== True or i['attributes']['acta_de_colindancia']== False else False
            actestratoid		= i['attributes']['estrato'] if i['attributes']['estrato']!= None else 7
            actotroestrato		= i['attributes']['otro_estrato_'] if i['attributes']['otro_estrato_']!= None else 0
            actrelacionpredioid	= i['attributes']['actrelacionpredioid'] if i['attributes']['actrelacionpredioid'] != None else 1   
            actdomicilionotificaciones	= i['attributes']['act_domici'] if i['attributes']['act_domici']  != None else 'No registra' 
            actprimernombre= i['attributes']['primer_nombre'] if i['attributes']['primer_nombre'] != None else 'No registra'
            actsegundonombre= i['attributes']['segundo_nombre'] if i['attributes']['segundo_nombre'] != None else 'No registra'
            actprimerapellido	= i['attributes']['primer_apellido'] if i['attributes']['primer_apellido'] != None else 'No Registra' 
            actsegundoapellido	= i['attributes']['segundo_apellido'] if i['attributes']['segundo_apellido'] != None else 'No Registra' 
            actcorreoelectronico = i['attributes']['act_correo'] if i['attributes']['act_correo'] != None else 'No registra' 
            actcelular= i['attributes']['act_celula'] if  i['attributes']['act_celula'] != None else 'No registra' 
            actnumerodocumento= i['attributes']['act_num_do'] if i['attributes']['act_num_do'] != None else 'No registra'
            actautorizanotificaciones = True if i['attributes']['autoriza_notificaciones'] == 1 else False
            


                     
            
            selectActPredio = 'SELECT actpredioid,actpredionumpred  FROM public.act_predio WHERE actpredionumpred = \'{}\';'.format(npn)
            actterrenogeo = i['geometry']['rings']
            for ir in actterrenogeo:
                rings=[]
                for ri in ir:
                    irr = "{} {}".format(ri[0],ri[1])
                    rings.append(irr)
                polygonRings=('MULTIPOLYGON(('+str(rings).replace('\'','').replace('[','(').replace(']',')')+'))')
                # print(polygonRings)
            
            if actestadoviaid == 0:
                actestadoviaid = 1

            # print(selectActPredio)
            cur.execute(selectActPredio)
            register = cur.fetchall()
            register = register[0] if len(register)>1 else register
            t_actpredio += len(register)

            # print(register)
            if len(register)>0:
                
                # print("Llenando Terreno...")
                # print(register)
                for i in register:
                    if type(i) == tuple:
                       
                        predioid=i[0] 
                        # print(predioid)
                        terrenoSelect = "SELECT actterrenoid from public.act_terreno where actpredioid = {};".format(predioid)
                        updateTerreno = "UPDATE public.act_terreno	SET actterrenousumod='{}',actterrenousucre='{}',actterrenoobsrec='{}',actnumeropredial='{}',actinfluenciaviaid={},acttipoviaid={},actmanzanaveredacod={},actareadigitalterreno={},actterrenoipmod='{}',actestadoviaid={},acttopografiaid={},actterrenofechmod='{}',actley56={},actdestinacionid={},actareaterreno={},actterrenofechcre='{}' WHERE actpredioid={};".format(actterrenousumod, actterrenousucre,actobservaciones,npn,actinfluenciaviaid,acttipoviaid,npn[14:17],actareadigitalterreno,actterrenoipmod,actestadoviaid,acttopografiaid,actterrenofechmod,actley56,actdestinacionid,actareaterreno,actterrenofechcre,predioid)
                        insertTerreno = "INSERT INTO public.act_terreno (actpredioid,actareaterreno,actareadigitalterreno,actavaluoterreno,actmanzanaveredacod,actnumeropredial,actley56,acttipoviaid,actestadoviaid,actinfluenciaviaid,acttopografiaid,actdestinacionid,actterrenoipmod,actterrenofechmod,actterrenousumod,actterrenoipcre,actterrenofechcre,actterrenousucre,actterrenodig,actterrenoobsrec,actterrenoestrec,actterrenoobsestdig,actterrenoestdig,actterrenogeo)VALUES ({},{},{},1,{},'{}',{},{},{},{},{},{},'{}','{}','{}','{}','{}','{}',False,'{}','f','sin observaciones','f', \'{}\') returning actterrenoid;".format(predioid,actareaterreno,actareadigitalterreno,npn[14:17],npn,actley56,acttipoviaid,actestadoviaid,actinfluenciaviaid,acttopografiaid,actdestinacionid,actterrenoipmod,actterrenofechmod,actterrenousumod,actterrenoipcre,actterrenofechcre,actterrenousucre,actobservaciones,polygonRings)

                        actpuntocardinalid =1
                        for index,lin in enumerate(linderos):
                            # print(index,lin)
                            actpuntocardinalid = index+1
                            insertLinderos = "INSERT INTO public.act_linderos (actpredioid,actpuntocardinalid,actlinderodes,actlinderoipmod,actlinderofechmod,actlinderousumod,actlinderoactpredioipcre,actlinderofechcre,actlinderousucre)	VALUES ({},{},'{}','{}','{}','{}','{}','{}','{}');".format(predioid,actpuntocardinalid,lin,actterrenoipmod,actterrenofechmod, actterrenousumod, actterrenousucre,  actterrenofechcre, actterrenousucre )
                            # print(insertLinderos)
                            cur.execute(insertLinderos)
                        #print(terrenoSelect)
                        cur.execute(terrenoSelect)

                        #print(cur.fetchone())
                        terrenoUpdates=cur.fetchone() if cur.fetchone() else 0 
                        #este no funciona
                        
                        if terrenoUpdates:
                            # print(terrenoUpdates)
                            if len(terrenoUpdates) > 0:
                                # print(updateTerreno)
                                try:
                                    cur.execute(updateTerreno)
                                    ut+=1
                                except:
                                    app.logger.error("Error en Update a la tabla terreno con la sentencia: , {}".format(updateTerreno))
                                # print('Terreno Updated', updateTerreno)
                        else:
                            try:
                                cur.execute(insertTerreno)
                                t_insert +=1
                            except:
                                app.logger.error("Error al impactar la tabla terreno con la sentencia: , {}".format(updateTerreno))
                        
                        # print(res)
                        insertDatosLevantamiento = "INSERT INTO public.act_datoslevantamiento (actpredioid,acttienearearegistra,actarearegistral,actproccatastralregistralid,actobservaciones,actfechavisitapredial,tipoidentificacion_id,actnumdocreconocedor,actprimernomreconocedor,actsegnomreconocedor,actprimerapellrecon,actsegapellrecon,actresultadovisitaid,actotroresultadovisita,actsuscribeactacolin,actdespojoabandono,actestratoid,actotroestrato,act_datoslevantamientoipmod,act_datoslevantamientofechmod,act_datoslevantamientousumod,act_datoslevantamientoipcre,act_datoslevantamientofechcre,act_datoslevantamientousucre)	VALUES ({},{},{},{},'{}','{}',{},'{}','{}','{}','{}','{}',{},'{}',{},{},{},'{}','{}','{}','{}','{}','{}','{}') returning actdatoslevantamientoid;".format(predioid,acttienearearegistra,actarearegistral,actproccatastralregistralid,actobservaciones,actfechavisitapredial,tipoidentificacion_id,actnumdocreconocedor,actprimernomreconocedor,actsegnomreconocedor,actprimerapellrecon,actsegapellrecon,actresultadovisitaid,actotroresultadovisita,actsuscribeactacolin,actdespojoabandono,actestratoid,actotroestrato,actterrenoipmod,actterrenofechmod,actterrenousumod,actterrenoipcre,actterrenofechcre,actterrenousucre)
                        try:
                            cur.execute(insertDatosLevantamiento)
                        except:
                            app.logger.error("Error en el Insert a la tabla datos levantamiento con la sentencia: {} ".format(insertDatosLevantamiento))
                        actdatoslevantamientoid= cur.fetchone()[0] #if cur.fetchone()[0] != None else 4341 
                        # print('datosId:_ ',actdatoslevantamientoid)
                        insertContactoVisita="INSERT INTO public.act_contactovisita (actautorizanotificaciones,actcorreoelectronico,actcelular,actdomicilionotificaciones,actrelacionpredioid,actsegundoapellido,actprimerapellido,actsegundonombre,actprimernombre,actnumerodocumento,actdatoslevantamientoid,actcontactovisitaipmod,actcontactovisitafechmod,actcontactovisitausumod,actcontactovisitaipcre,actcontactovisitafechcre,actcontactovisitausucre)	VALUES ({},'{}','{}','{}',{},'{}','{}','{}','{}','{}',{},'{}','{}','{}','{}','{}','{}');".format(actautorizanotificaciones,actcorreoelectronico,actcelular,actdomicilionotificaciones,actrelacionpredioid,actsegundoapellido,actprimerapellido,actsegundonombre,actprimernombre,actnumerodocumento,actdatoslevantamientoid,actterrenoipmod,actterrenofechmod,actterrenousumod,actterrenoipcre,actterrenofechcre,actterrenousucre)
                        # print(insertContactoVisita)
                        try:
                            cur.execute(insertContactoVisita)
                        except:
                            app.logger.error("Error en el Insert a la tabla ContactoVisita, con la sentencia: {}".format(insertContactoVisita))
                            
                        conn.commit()


            
                        for v in ConstruccionFeatures:                       
                        
                            actnumeropisos		=	v['attributes']['act_numero_pisos'] if v['attributes']['act_numero_pisos'] != "null" and v['attributes']['act_numero_pisos']!= None else 0
                            actnumerosotanos		=	v['attributes']['act_numero_sotanos'] if v['attributes']['act_numero_sotanos'] != "null" and v['attributes']['act_numero_sotanos']!= None  else 0
                            actnumeromezani		=	 v['attributes']['act_numero_mezani'] if v['attributes']['act_numero_mezani'] != "null" and v['attributes']['act_numero_mezani']!= None  else 0
                            actnumerosemisotanos		=	 v['attributes']['act_numero_semisotanos'] if v['attributes']['act_numero_semisotanos'] != "null" and v['attributes']['act_numero_semisotanos']!= None  else 0
                            actanoconstruccion		= v['attributes']['act_anio_construccion'] if v['attributes']['act_anio_construccion'] != "null" and  v['attributes']['act_anio_construccion'] != None  else 0
                            actavaluoconstruccion		=	v['attributes']['act_avaluo_construccion'] if v['attributes']['act_avaluo_construccion']  != "null" and v['attributes']['act_avaluo_construccion']!= None  else 0
                            actareaplanoconstruccion		=	 v['attributes']['act_area_plano_construccion'] if v['attributes']['act_area_plano_construccion']  != "null" and  v['attributes']['act_area_plano_construccion']!= None  else 0
                            actconaltura	=   v['attributes']['act_altura'] if v['attributes']['act_altura'] != "null"  and v['attributes']['act_altura']!= None else 0
                            actconobservaciones		=	v['attributes']['act_observaciones'][0:100].replace("'","") if v['attributes']['act_observaciones']  != "null" and  v['attributes']['act_observaciones']  != None else "Sin observaciones"
                            actareadigitalconstruccion=v['attributes']['SHAPE__Area'] if v['attributes']['SHAPE__Area'] != "null"  and v['attributes']['SHAPE__Area']!= None   else 0
                            act_pkconstruccion = '{}'.format(v['attributes']['pk_constru']) if v['attributes']['pk_constru'] != "null"  and v['attributes']['pk_constru']!= None  else 0
                            codigo = v['attributes']['codigo']
                            actconstruccionipmod = v['attributes']['last_edited_user'] if v['attributes']['last_edited_user'] != "null" and v['attributes']['last_edited_user']!= None   else 0
                            actconstruccionfechmod = datetime.fromtimestamp((v['attributes']['last_edited_date'])/1000) if v['attributes']['last_edited_date'] != "null"  and v['attributes']['last_edited_date'] != None  else 0
                            actconstruccionusumod = v['attributes']['last_edited_user'] if v['attributes']['last_edited_user']  != "null" and v['attributes']['last_edited_user'] != None   else 0
                            actconstruccionipcre = v['attributes']['created_user'] if v['attributes']['created_user']  != "null"  and v['attributes']['created_user']!= None else 0
                            actconstruccionfechcre = datetime.fromtimestamp((v['attributes']['created_date'])/1000) if  v['attributes']['created_date']  != "null"  and  v['attributes']['created_date']!= None else 0
                            actconstruccionusucre = v['attributes']['created_user'] if v['attributes']['created_user'] != "null"  and v['attributes']['created_user'] != None else 0
                            nul = 'null'
                            default =0
                            try: 
                                    
                                actterrenogeo = v['geometry']['rings']
                                for ir in actterrenogeo:
                                    rings=[]
                                    for ri in ir:
                                        irr = "{} {}".format(ri[0],ri[1])
                                        rings.append(irr)
                                    polygonRingsV=('MULTIPOLYGON(('+str(rings).replace('\'','').replace('[','(').replace(']',')')+'))')
                            except:
                                polygonRingsV='POLYGON((0 0, 0 1, 1 1, 1 0))'
    

                            if(codigo == npn and str(act_pkconstruccion) in unidadCodigos):
                                uc+=1
                                # print(codigo, npm )

                                insertConstruccion = "INSERT INTO public.act_construccion (actpredioid,actidentificador,actnumeropisos,actnumerosotanos,actnumeromezani,actnumerosemisotanos,actanoconstruccion,actavaluoconstruccion,actareaplanoconstruccion,actareadigitalconstruccion,actconaltura,actconobservaciones,actusuacrea,actconstruccionipmod,actconstruccionfechmod,actconstruccionusumod,actconstruccionipcre,actconstruccionfechcre,actconstruccionusucre,act_pkconstruccion,actconstruccionestdig,actconstrucciondig,actconstruccionobsrec,actconstruccionestrec,actconstruccionobsestdig,actconstrucciongeo) VALUES ({},\'{}\',{},{},{},{},{},{},{},{},{},\'{}\',{},\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'L\',False,\'Sin Observaciones\',\'F\',\'Sin Observaciones\',\'{}\') RETURNING act_pkconstruccion, actpredioid;".format(predioid,nul,actnumeropisos,actnumerosotanos,actnumeromezani,actnumerosemisotanos,actanoconstruccion,actavaluoconstruccion,actareaplanoconstruccion,actareadigitalconstruccion,actconaltura,actconobservaciones,default,actconstruccionipmod,actconstruccionfechmod,actconstruccionusumod,actconstruccionipcre,actconstruccionfechcre,actconstruccionusucre,act_pkconstruccion,polygonRingsV)
                                #print(insertConstruccion)
                                if actinfluenciaviaid !=0:
                                    try:
                                        cur.execute(insertConstruccion)
                                    except:
                                        app.logger.error("Error en el Insert a la tabla Construcción con la sentencia: {}".format(insertConstruccion))
                                    
                                    conn.commit()
        app.logger.info('terreno procedimiento no nulo: {}'.format(t_proc_notnnull))
        app.logger.info('terreno en act_predio: {}'.format(t_actpredio))
        app.logger.info('terreno inserts: {}'.format(t_insert))
        app.logger.info('terreno updates: {}'.format(ut))
        app.logger.info('construcciones con codigo en unidad insert :{}'.format(uc))
        return 0

    def Impacto_unidad_calificaciones(UnidadConstruccionFeatures):
        iuc=0
        
        controlCount=0
        for i in UnidadConstruccionFeatures:
           # print(i)
            pkc = i['attributes']['pk_constru']


            if pkc:
                
                
                select = 'SELECT actconstruccionid,act_pkconstruccion,actpredioid FROM public.act_construccion WHERE  act_pkconstruccion = \'{}\';'.format(
                    pkc)
                cur.execute(select)  # deberia traer resultado_visita_id
                
                register = cur.fetchone()
                # print(register)

                act_predioid = 0
                if register != None:
                    act_predioid = register[2]
                    actplantatipoid = i['attributes']['act_tipo_planta'] if i['attributes']['act_tipo_planta'] else 1
                    actunidadconsttipoid = i['attributes']['act_tipo_unidad_construccion'] +1 if i[
                        'attributes']['act_tipo_unidad_construccion'] !=None else None
                    acttipoconstruccionid = 2 if i['attributes']['act_tipo_construccion'] == 1 else 1
                    actgasdomiciliario = True if i['attributes']['act_gas'] == 1 else False
                    acttelefono = True if i['attributes']['act_telefono'] == 1 else False
                    actenergiaelectrica = True if i['attributes']['act_energia'] == 1 else False
                    actalcantarillado = True if i['attributes']['act_alcantarillado'] == 1 else False
                    actacueducto = True if i['attributes']['act_alcantarillado'] == 1 else False
                    actuniconsobservaciones = i['attributes']['act_observaciones'] if i['attributes']['act_observaciones']  !=None else 0
                    actareaprivadaconstruida = i['attributes']['act_area_privada'] if i['attributes']['act_area_privada'] !=None  else 0
                    actanioconstruccion = i['attributes']['act_anio_construccion'] if i['attributes']['act_anio_construccion']  !=None else 0
                    acttotalplantasunidad = i['attributes']['act_total_plantas_unidad'] if i[
                        'attributes']['act_total_plantas_unidad']  !=None else 0
                    acttotallocales = i['attributes']['act_total_locales'] if i['attributes']['act_total_locales'] !=None  else 0
                    acttotalbanos = i['attributes']['act_total_banos'] if i['attributes']['act_total_banos']  !=None else 0
                    acttotalhabitaciones = i['attributes']['act_total_habitaciones'] if i[
                        'attributes']['act_total_habitaciones']  !=None else 0
                    acttipodominioid = 2 if i['attributes']['act_tipo_de_dominio']  ==1 else 1
                    actaltura = i['attributes']['act_altura'] if i['attributes']['act_altura']  !=None else 0
                    actareaprivadacontruidaph = i['attributes']['act_area_privada_cons'] if i[
                        'attributes']['act_area_privada_cons']  !=None else 0
                    actareadigitalconstruida = i['attributes']['SHAPE__Area'] if i['attributes']['SHAPE__Area']  !=None else 0
                    actareaconstruida = i['attributes']['act_area_construida'] if i['attributes']['act_area_construida']  !=None else 0
                    actplantaubicacion = i['attributes']['act_planta_ubicacion'] if i['attributes'][
                        'act_planta_ubicacion'] != None and i['attributes']['act_planta_ubicacion'] != "null" else 0
                    actavaluounidadconstruccion = i['attributes']['act_avaluo_unidad'] if i['attributes'][
                        'act_avaluo_unidad'] != None and i['attributes']['act_avaluo_unidad'] != 'null' else 0
                    usoid = i['attributes']['act_tipo_unidad_construccion'] if i['attributes'][
                        'act_tipo_unidad_construccion'] != None and i['attributes']['act_tipo_unidad_construccion'] != "null" else 0
                    actunidadcontruccionipmod = i['attributes']['last_edited_user'] if i['attributes'][
                        'last_edited_user'] != None and i['attributes']['last_edited_user'] != "null" else "No registra"
                    actunidadcontruccionusumod = i['attributes']['last_edited_user'] if i['attributes'][
                        'last_edited_user'] != None and i['attributes']['last_edited_user'] != "null" else "No registra"
                    actunidadcontruccionfechedit = datetime.fromtimestamp(
                        (i['attributes']['last_edited_date'])/1000)
                    actunidadcontruccionfechcrea = datetime.fromtimestamp(
                        (i['attributes']['created_date'])/1000)
                    actunidadcontruccionusucre = i['attributes']['created_user'] if i['attributes'][
                        'created_user'] != None and i['attributes']['created_user'] != "null" else "No registra"
                    # # # # # # # # # # # # # # # # # # # # # # #            C A L I F I C A C I O N E S                 # # # # # # # # # # # # # # # # # # # # # # # #

                    # No convencional
                    anexotipo_id = i['attributes']['act_tipo_anexo'] +1  if i['attributes']['act_tipo_anexo'] else None
                    actpuntaje = i['attributes']['act_puntaje_noconvencional']
                    # Tipología Convencional
                    tipologiatipo_id = i['attributes']['act_tipo_tipologia'] if i['attributes']['act_tipo_tipologia'] else 25
                    actotrotipologia = i['attributes']['act_otro_tipo'] if i['attributes']['act_otro_tipo'] else "No registra"
                    # califConvercional
                    #   CLASES
                    # estructura
                    estructura_estadoconservacion_id = (
                        (i['attributes']['estructura_grupocalif4'])-15) if i['attributes']['estructura_grupocalif4'] != 'null' and i['attributes']['estructura_grupocalif4'] != None else None
                    estructura_cubierta_id = ((i['attributes']['estructura_grupocalif3']) +
                                            1) if i['attributes']['estructura_grupocalif3'] and i['attributes']['estructura_grupocalif3'] != 'null' else None
                    estructura_muros_id = ((i['attributes']['estructura_grupocalif2']) +
                                        1) if i['attributes']['estructura_grupocalif2'] and i['attributes']['estructura_grupocalif2'] != 'null' else None
                    estructura_armazon_id = ((i['attributes']['estructura_grupocalif']) +
                                            1) if i['attributes']['estructura_grupocalif'] and i['attributes']['estructura_grupocalif'] != 'null' else None
                    estructura = [estructura_armazon_id,
                                estructura_muros_id, estructura_cubierta_id]

                    # acabados
                    acabados_erstadoconservacion_id = (
                        (i['attributes']['acabados_grupocalif3']) - 15) if i['attributes']['acabados_grupocalif3'] and i['attributes']['acabados_grupocalif3'] != 'null' else None
                    acabados_pisos_id = ((i['attributes']['acabados_grupocalif2']) -
                                        3) if i['attributes']['acabados_grupocalif2'] and i['attributes']['acabados_grupocalif2'] != 'null' else None
                    acabados_curbimientoMuros_id = (
                        (i['attributes']['acabados_grupocalif1']) - 3) if i['attributes']['acabados_grupocalif1'] and i['attributes']['acabados_grupocalif1'] != 'null' else None
                    acabados_fachada_id = ((i['attributes']['acabados_grupocalif0']) -
                                        3) if i['attributes']['acabados_grupocalif0'] and i['attributes']['acabados_grupocalif0'] != 'null' else None
                    acabados = [acabados_fachada_id,
                                acabados_curbimientoMuros_id, acabados_pisos_id]

                    # baños
                    baño_estadoConservacion_id = ((i['attributes']['bano_grupocalif3']) - 15) if i['attributes']['bano_grupocalif3'] and i['attributes']['bano_grupocalif3'] != 'null' else None
                    baño_mobiliario_id = ((i['attributes']['bano_grupocalif2']) - 3) if i['attributes']['bano_grupocalif2'] and i['attributes']['bano_grupocalif2'] != 'null' else None
                    baño_enchapes_id = ((i['attributes']['bano_grupocalif1']) - 3) if i['attributes']['bano_grupocalif1'] and i['attributes']['bano_grupocalif1'] != 'null' else None
                    baño_tamaño_id = ((i['attributes']['bano_grupocalif0']) - 3) if i['attributes']['bano_grupocalif0'] and i['attributes']['bano_grupocalif0'] != 'null' else None
                    baños = [baño_tamaño_id, baño_enchapes_id, baño_mobiliario_id]

                    # cocinas
                    cocina_estadoConservacion_id = (
                        (i['attributes']['cocina_grupocalif3']) - 15) if i['attributes']['cocina_grupocalif3'] and i['attributes']['cocina_grupocalif3'] != 'null' else None
                    cocina_mobiliario_id = ((i['attributes']['cocina_grupocalif2']) -
                                            3) if i['attributes']['cocina_grupocalif2'] and i['attributes']['cocina_grupocalif2'] != 'null' else None
                    cocina_enchapes_id = ((i['attributes']['cocina_grupocalif1']) -
                                        3) if i['attributes']['cocina_grupocalif1'] and i['attributes']['cocina_grupocalif1'] != 'null' else None
                    cocina_tamaño_id = ((i['attributes']['cocina_grupocalif0']) - 3) if i['attributes']['cocina_grupocalif0'] and i['attributes']['cocina_grupocalif0'] != 'null' else None
                    cocinas = [cocina_tamaño_id, cocina_enchapes_id, cocina_mobiliario_id]


                    #Usos
                    usoResideincial=i['attributes']['act_uso_construccin_residencial']  
                    usoComercial=i['attributes']['act_uso_construccion_comercial']
                    usoAnexo=i['attributes']['act_uso_construccion_anexo']
                    usoIndustrial=i['attributes']['act_uso_construccin_industrial']
                    usoInstitucional=i['attributes']['act_uso_cons_institucional']
                
                    if usoResideincial != None and usoAnexo ==None and usoIndustrial ==None and usoInstitucional==None and usoComercial ==None:
                        tipo = 'res'
                    elif usoResideincial == None and usoAnexo ==None and usoIndustrial ==None and usoInstitucional==None and usoComercial !=None:
                        tipo = 'com'
                    elif usoResideincial == None and usoAnexo ==None and usoIndustrial!=None and usoInstitucional==None and usoComercial ==None:
                        tipo = 'com'
                    elif usoResideincial == None and usoAnexo ==None and usoIndustrial ==None and usoInstitucional!=None and usoComercial ==None:
                        tipo = 'com'
                    else:
                        tipo = ''
                    #Geometria
                    
                    actterrenogeo = i['geometry']['rings']
                    for ir in actterrenogeo:
                        rings=[]
                        for ri in ir:
                            irr = "{} {}".format(ri[0],ri[1])
                            rings.append(irr)
                        polygonRings=('MULTIPOLYGON(('+str(rings).replace('\'','').replace('[','(').replace(']',')')+'))')
                        
                    actconstruccionid = register[0]
                    
                    UnidasdInsert = 'INSERT INTO public.act_unidadconstruccion (actconstruccionid,actplantaubicacion,actareaconstruida,actareadigitalconstruida,actareaprivadacontruidaph,actaltura,acttipodominioid,acttipoconstruccionid,actunidadconsttipoid,actplantatipoid,acttotalhabitaciones,acttotalbanos,acttotallocales,acttotalplantasunidad,actanioconstruccion,actavaluounidadconstruccion,actareaprivadaconstruida,actuniconsobservaciones,actacueducto,actalcantarillado,actenergiaelectrica,acttelefono,actgasdomiciliario,usoid,actunidadcontruccionipmod,actunidadcontruccionusumod,actunidadcontruccionipcre,actunidadcontruccionfechcre,actunidadcontruccionusucre,actunidadcontruccionfechmod,act_predioid,actunidadconstruccionestrec,actunidadconstruccionestdig,actunidadconstruccionobsrec,actunidadconstrucciondig,actunidadconstruccionobsestdig,actunidadconsgeo) VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},\'{}\',{},{},{},{},{},{},\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',{},\'L\',\'L\',\'Sin Observaciones\',False,\'NULL\',\'{}\') returning actunidadcontruccionid;'.format(
                        actconstruccionid, actplantaubicacion, actareaconstruida, actareadigitalconstruida, actareaprivadacontruidaph, actaltura, acttipodominioid, acttipoconstruccionid, actunidadconsttipoid, actplantatipoid, acttotalhabitaciones, acttotalbanos, acttotallocales, acttotalplantasunidad, actanioconstruccion, actavaluounidadconstruccion, actareaprivadaconstruida, actuniconsobservaciones, actacueducto, actalcantarillado, actenergiaelectrica, acttelefono, actgasdomiciliario, usoid, actunidadcontruccionipmod, actunidadcontruccionusumod, actunidadcontruccionusucre, actunidadcontruccionfechedit, actunidadcontruccionusucre, actunidadcontruccionfechedit, act_predioid,polygonRings)
                    # print(UnidasdInsert)
                    try:
                        cur.execute(UnidasdInsert)
                        iuc+=1
                        actunidadcontruccionid = cur.fetchone()[0]
                    except:
                        app.logger.error("Insert a UnidadConstruccion, con la sentencia: {}".format(UnidasdInsert))
                    
                    # print(actunidadcontruccionid)

                    tipologia = 'INSERT INTO public.act_tipologiaconstruccion (actunidadcontruccionid,tipologiatipo_id,actotrotipologia,acttipologiaconstruccionipmod,acttipologiaconstruccionfechmod,acttipologiaconstruccionusumod,acttipologiaconstruccionipcre,acttipologiaconstruccionfechcre,acttipologiaconstruccionusucre) VALUES ({},{},\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\') returning 1;'.format(
                        actunidadcontruccionid, tipologiatipo_id, actotrotipologia, actunidadcontruccionipmod, actunidadcontruccionfechedit, actunidadcontruccionusumod, actunidadcontruccionusucre, actunidadcontruccionfechcrea, actunidadcontruccionusucre)
                    #print(tipologia)
                    try:
                        cur.execute(tipologia)
                        tipologiaId = cur.fetchone()[0]
                        controlCount+=tipologiaId
                    except:
                        app.logger.error("Insert a la tabla tipologia, con la sentencia: {}".format(tipologia))
                    
                    #print('Topologia Insertado: ', tipologiaId)
                    
                    def updateUsoUnidad(uso):
                        if uso:
                            
                            usoSelect="SELECT * FROM public.uso Where usoitfcode ={};".format(uso)

                            cur.execute(usoSelect)
                            response = cur.fetchall()
                            try:
                                usoitfcode = response[0][0]
                                unidadUpdate = "UPDATE public.act_unidadconstruccion	SET usoid={}	WHERE actunidadcontruccionid={};".format(usoitfcode,actunidadcontruccionid)
                                cur.execute(unidadUpdate)
                            except:
                                app.logger.warning("Registro de unidad con sentencia: {} no agregado ".format(unidadUpdate))
                                pass
                    
                    
                        
                    
                    # N O    C O N V E N C I O N A L
                    if usoid == 4 and anexotipo_id:
                        updateUsoUnidad(usoAnexo)
                        
                        # CALIFICACION NO CONVERNCIONAL 1
                        noConvencioalInsert = 'INSERT INTO public.act_califnoconvencion (actunidadcontruccionid,anexotipo_id,actpuntaje,actcalifnoconvencionaipmod,actcalifnoconvencionafechmod,actcalifnoconvencionausumod,actcalifnoconvencionaipcre,actcalifnoconvencionafechcre,actcalifnoconvencionausucre)VALUES ({},{},{},\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\') returning actcalifnoconvencionalid;'.format(
                            actunidadcontruccionid, anexotipo_id, actpuntaje, actavaluounidadconstruccion, actunidadcontruccionfechedit, actunidadcontruccionipmod, actunidadcontruccionusucre, actunidadcontruccionfechcrea, actunidadcontruccionusucre)
                        try:
                            cur.execute(noConvencioalInsert)
                            actunidadcontruccionid = cur.fetchone()
                        except:
                            app.logger.error("Error al insertar registro: {} en la tabla act_califnoconvencion". format(noConvencioalInsert) )
                        #print('No Convencioanal Insertado: ',
                        #    actunidadcontruccionid)

                    # C  O N V E N C I O N A L

                    elif estructura_armazon_id and acabados_fachada_id and acabados_erstadoconservacion_id and estructura_estadoconservacion_id:

                        if usoid == 0:
                            ACT_tipo_calificacion_id = 1
                            
                            updateUsoUnidad(usoResideincial)
                            
                        elif usoid == 1 or usoid == 3:
                            ACT_tipo_calificacion_id = 3
                            if usoInstitucional:
                                updateUsoUnidad(usoInstitucional)
                            elif usoComercial:
                                updateUsoUnidad(usoComercial)
                            
                        elif usoid == 2:
                            ACT_tipo_calificacion_id = 2
                            updateUsoUnidad(usoIndustrial)
                            

                        califConvencional = 'INSERT INTO public.act_califconvencional (calificartipo_id,acttotalcalificacion,actunidadcontruccionid,actcalifconvencionaipmod,actcalifconvencionafechmod,actcalifconvencionausumod,actcalifconvencionaipcre,actcalifconvencionafechcre,actcalifconvencionausucre)	VALUES ({},{},{},\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\') returning actcalifconvencionalid;'.format(
                            ACT_tipo_calificacion_id, 0, actunidadcontruccionid, actunidadcontruccionipmod, actunidadcontruccionfechedit, actunidadcontruccionusumod, actunidadcontruccionusucre, actunidadcontruccionfechcrea, actunidadcontruccionusucre)
                        try:
                            cur.execute(califConvencional)
                            actcalifconvencionalid = cur.fetchone()[0]
                        except:
                            app.logger.error("Error al insertar registro: {} en la tabla act_califconvencional". format(califConvencional))
                        
                        def populateGrupo_Object(groupArray, code, estado_conservacion):
                            
                            puntajeCount = 0
                            if groupArray and code and estado_conservacion:

                                grupoCalificcion = 'INSERT INTO public.act_grupocalificacion (actcalifconvencionalid,clasecaliftipo_id,estadoconservacion_id,actsubtotal,actgrupocalifipmod,actgrupocaliffechmod,actgrupocalifusumod,actgrupocalifipcre,actgrupocaliffechcre,actgrupocalifusucre)	VALUES ({},{},{},0,\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\') returning actgrupocalifid;'.format(
                                    actcalifconvencionalid, code, estado_conservacion, actunidadcontruccionipmod, actunidadcontruccionfechedit, actunidadcontruccionusumod, actunidadcontruccionusucre, actunidadcontruccionfechcrea, actunidadcontruccionusucre)
                                # print(grupoCalificcion)
                                try:
                                    cur.execute(grupoCalificcion)
                                    actgrupocalifid = cur.fetchone()[0]
                                except:
                                    app.logger.error("Error al insertar registro: {} en la tabla act_califnoconvencion". format(grupoCalificcion))
                                if estado_conservacion == 1:
                                    estadoConservacionPuntaje = 0
                                elif estado_conservacion == 2:
                                    estadoConservacionPuntaje = 2
                                elif estado_conservacion == 3:
                                    estadoConservacionPuntaje = 4
                                elif estado_conservacion == 4:
                                    estadoConservacionPuntaje = 5
                                    
                                for o in groupArray:
                                    if o:

                                        objetoConstruccionSearch = 'SELECT tipoobjetoconstru_puntos, tipoobjetoconstru_id FROM public.tipoobjetoconstru where tipoobjetoconstru_itfcode = {};'.format(o)
                                        # print(objetoConstruccionSearch)
                                        cur.execute(objetoConstruccionSearch)
                                        _res = cur.fetchall()
                                        tipoobjetoconstru_puntos=0
                                        tipoobjetoconstru_id=0
                                        if len(_res)>1:
                                            if tipo == 'res':
                                                tipoobjetoconstru_puntos = _res[0][0]
                                                tipoobjetoconstru_id = _res [0][1]
                                            elif tipo == 'com':
                                                tipoobjetoconstru_puntos = _res[1][0]
                                                tipoobjetoconstru_id = _res [1][1]
                                            else:
                                                pass
                                        else:
                                            # print(_res[0][0])
                                            tipoobjetoconstru_puntos=_res[0][0]
                                            # print(_res[0][1])
                                            tipoobjetoconstru_id=_res[0][1]
                                        
                                        # print(tipoobjetoconstru_puntos)
                                        puntajeCount += tipoobjetoconstru_puntos

                                        objetoConstruccionInsert = 'INSERT INTO public.act_objetoconstruccion (actgrupocalifid,actppuntos,actobjetoconstruccionipmod,actobjetoconstruccionfechmod,actobjetoconstruccionusumod,actobjetoconstruccionipcre,actobjetoconstruccionfechcre,actobjetoconstruccionusucre,tipoobjetoconstru_id)	VALUES ({},{},\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',{}) returning actobjetoconstruccionid;'.format(
                                            actgrupocalifid, tipoobjetoconstru_puntos, actunidadcontruccionipmod, actunidadcontruccionfechedit, actunidadcontruccionusumod, actunidadcontruccionusucre, actunidadcontruccionfechcrea, actunidadcontruccionusucre, tipoobjetoconstru_id)
                                        try:
                                            cur.execute(
                                            objetoConstruccionInsert)
                                        except:
                                            app.logger.error("Error al insertar registro: {} en la tabla act_objetoconstruccion". format(objetoConstruccionInsert))
                                    updateCalificacionSubTotal = 'UPDATE public.act_grupocalificacion SET actsubtotal={} WHERE actgrupocalifid={};'.format(
                                        puntajeCount, actgrupocalifid)
                                    cur.execute(
                                        updateCalificacionSubTotal)
                                    
                                    return puntajeCount+estadoConservacionPuntaje
                            else:
                                # print('Datos No validos: ', groupArray,
                                    # code, estado_conservacion)
                                return puntajeCount

                        estructuraTotal = populateGrupo_Object(
                            estructura, 1, estructura_estadoconservacion_id)
                        acabadosTotal = populateGrupo_Object(
                            acabados, 2, acabados_erstadoconservacion_id)
                        bañosTotal = populateGrupo_Object(baños, 3, baño_estadoConservacion_id)
                        cocinasTotal = populateGrupo_Object(
                            cocinas, 4, cocina_estadoConservacion_id)
                        totalCalificacion = (cocinasTotal+bañosTotal+acabadosTotal+estructuraTotal)

                        updateCalificacionTotal = 'UPDATE public.act_califconvencional	SET acttotalcalificacion={}	WHERE actcalifconvencionalid={} returning actcalifconvencionalid;'.format(
                            totalCalificacion, actcalifconvencionalid)
                        cur.execute(updateCalificacionTotal)
                        FinishControlId = cur.fetchone()[0]
                        # print('Calificacion Fianlizada: ',
                            # FinishControlId)

                    conn.commit()
                    
                    control.append(i)
        app.logger.info("unidades validas e insertadas: {}".format(iuc))

    

    cleanTables()
    impactDatosLevantamiento(ofertaFeatures)
    Impacto_terrenos_construcciones(terrenoFeatures,ConstruccionFeatures,UnidadConstruccionFeatures)
    Impacto_unidad_calificaciones(UnidadConstruccionFeatures)
    
    


updateFieldsToDb()
# scheduler = BackgroundScheduler()
# trigger = CronTrigger(
#         year="*", month="*", day="*", hour="0", minute="0", second="0"
#     )
# scheduler.add_job(func=updateFieldsToDb, trigger=trigger)#86400)
# scheduler.start()
# atexit.register(lambda: scheduler.shutdown())



@app.route('/basic_api/entities', methods=['GET', 'POST'])
def entities():
    if request.method == "GET":
        return {
            'message': 'This endpoint should return a list of entities',
            'method': request.method
        }
    if request.method == "POST":
        return {
            'message': 'This endpoint should create an entity',
            'method': request.method,
            'body': request.json
        }


# @app.route('/basic_api/entities/<int:entity_id>', methods=['GET', 'PUT', 'DELETE'])
# def entity(entity_id):
#     if request.method == "GET":
#         return {
#             'id': entity_id,
#             'message': 'This endpoint should return the entity {} details'.format(entity_id),
#             'method': request.method
#         }
#     if request.method == "PUT":
#         return {
#             'id': entity_id,
#             'message': 'This endpoint should update the entity {}'.format(entity_id),
#             'method': request.method,
#             'body': request.json
#         }
#     if request.method == "DELETE":
#         return {
#             'id': entity_id,
#             'message': 'This endpoint should delete the entity {}'.format(entity_id),
#             'method': request.method
#         }
