import requests
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import sys
from os import system

class ticker_base_datos:

    def __init__(self, nombre_bd="TickerBaseDatos.db"):
        self.nombre_bd = nombre_bd
        self.format = "%Y-%m-%d"
        self.conn = sqlite3.connect(self.nombre_bd)
        self.cursor = self.conn.cursor()
        self.tabla_principal_crear()

    def tabla_principal_crear(self):
        try:
            self.cursor.execute('''CREATE TABLE IF NOT EXISTS TickerGuardados (
                                   Ticker TEXT PRIMARY KEY, 
                                   FechaInicio TEXT, 
                                   FechaFinal TEXT
                                )''')
            self.conn.commit()
        except sqlite3.OperationalError:
            return None
    
    def tabla_principal_insertar(self, nombre_ticker, fecha_inicio,fecha_final):
        self.conn.execute (f'''INSERT INTO TickerGuardados (Ticker, FechaInicio, FechaFinal) 
                                       VALUES ('{nombre_ticker}','{fecha_inicio}','{fecha_final}');
                              ''')  
        self.conn.commit()
        self.tabla_principal_ordenar()

    def tabla_principal_actuaizar(self, nombre_ticker, fecha_inicio, fecha_final):
        self.conn.execute (f'''UPDATE TickerGuardados
                        SET FechaInicio = '{fecha_inicio}',
                            FechaFinal = '{fecha_final}'
                        WHERE Ticker = '{nombre_ticker}'
                       ''')  
        self.conn.commit()
            
    def tabla_principal_ordenar(self):     
        self.conn.execute (f''' CREATE TABLE IF NOT EXISTS Ticker_Ordenados AS SELECT * FROM TickerGuardados ORDER BY Ticker''')
        self.conn.execute (f''' DROP TABLE IF EXISTS TickerGuardados''')
        self.conn.execute (f''' ALTER TABLE Ticker_Ordenados RENAME TO TickerGuardados''')
        self.conn.commit()
    
    def tabla_principal_buscar (self):          
        df=pd.read_sql(con=self.conn,sql="SELECT * FROM TickerGuardados")
        lista_ticker_guardados=list(df.Ticker)
        return lista_ticker_guardados, df 

    def tabla_princal_visualizar(self):
        print("LISTADO DE TICKERS ALMACENADOS EN BASE DATOS\n")
        df=pd.read_sql(con=self.conn,sql="SELECT * FROM TickerGuardados")
        print(f'\tTICKER\t-\tFECHA INICIO\t<->\tFECHA FINAL\n')
        for i in range (0,len(df.Ticker)):
                print(f'\t{df.Ticker[i]}\t-\t{df.FechaInicio[i]}\t<->\t{df.FechaFinal[i]}')
        self.conn.close()
    
    def tabla_principal_borrar_registro(self, nombre_ticker):
        self.conn.execute (f'''DELETE FROM TickerGuardados
                       WHERE Ticker = '{nombre_ticker}';
                              ''')  
        self.conn.commit()
                 
    def tabla_ticker_crear(self, resultados_API, nombre_ticker):
        self.cursor.execute (f'''CREATE TABLE {nombre_ticker}(
                        Fecha TEXT, VolumenOperado REAL, PrecioPromedioPorVolumen REAL, PrecioApertura REAL, PrecioCierre REAL, 
                        PrecioMásAlto REAL, PrecioMásBajo REAL, NúmeroDeTransacciones REAL
                        )    ''')
        self.conn.commit()
        self.tabla_ticker_insertar(resultados_API,nombre_ticker) 
        print("\n\t\tDATOS GUARDADOS CORRECTAMENTE")

    def tabla_ticker_insertar(self, resultados_API, nombre_ticker): 
        fechaDataString=[]
        for i in range(0,len(resultados_API)):
            fechaDataString.append(datetime.fromtimestamp(resultados_API[i]['t']/1000.0).date().strftime(self.format))
            self.conn.execute (f'''INSERT INTO {nombre_ticker} (Fecha, VolumenOperado, PrecioPromedioPorVolumen, PrecioApertura, 
                                                PrecioCierre, PrecioMásAlto, PrecioMásBajo, NúmeroDeTransacciones) 
                                VALUES ('   {fechaDataString[i]}',{resultados_API[i]['v']},{resultados_API[i]['vw']},
                                            {resultados_API[i]['o']},{resultados_API[i]['c']},{resultados_API[i]['h']},
                                            {resultados_API[i]['l']},{resultados_API[i]['n']});
                                ''')
            self.conn.commit()

    def tabla_ticker_ordenar(self, nombre_ticker):
        self.conn.execute (f''' CREATE TABLE IF NOT EXISTS Datos_Ordenados AS SELECT * FROM {nombre_ticker} ORDER BY Fecha ASC''')
        self.conn.execute (f''' DROP TABLE IF EXISTS {nombre_ticker}''')
        self.conn.execute (f''' ALTER TABLE Datos_Ordenados RENAME TO {nombre_ticker}''')
        self.conn.commit()
        
    def tabla_ticker_borrar(self, nombre_ticker):
        self.conn.execute (f'''DROP TABLE {nombre_ticker} ''')  
        self.conn.commit()

    def tabla_ticker_buscar(self, tickerPrint):
        return pd.read_sql(con=self.conn,sql=f"SELECT * FROM {tickerPrint}")
      
    def cerrar_bd(self):
        self.conn.close()

class ticker_actualizar:
    
    def __init__(self, ticker_db,sub_menu):
        self.ticker_db = ticker_db
        self.sub_menu = sub_menu
        self.format="%Y-%m-%d"
        
    def solicitar_datos_ticker(self, nombre_ticker, fecha_inicio, fecha_final):
        print("\n\t\tSOLICITANDO DATOS..")
        ticker_API_datos=requests.get(f"https://api.polygon.io/v2/aggs/ticker/{nombre_ticker}/range/1/day/{fecha_inicio}/{fecha_final}?apiKey=_E06pfStxn1XpmSEBG7HUwEeV7029dfW")
        resultados_API=ticker_API_datos.json()
        #MANEJO DE ERROR REQUESTS   
        if resultados_API['status']=='OK' and resultados_API.status_code == 200:
                return resultados_API['results']   #Esto es lo que devuelve                                                 
        elif resultados_API['status']=='NOT_AUTHORIZED':
                        print("\n\t\tSU PLAN NO INCLUYE ESTE PERÍODO DE DATOS PARA ESTE TICKER. INTENTE EN FECHAS POSTERIORES.\n")
                        self.sub_menu.consultaFinal()
        elif resultados_API.status_code == 403:
                        print("\n\t\tERROR! 403 FORBIDDEN")
                        self.sub_menu.consultaFinal()
        elif resultados_API.status_code == 404:
                        print("\n\t\tERROR! 404 NOT FOUND")
                        self.sub_menu.consultaFinal()

    def validar_ticker(self,nombre_ticker):
        ticker_API_datos=requests.get(f"https://api.polygon.io/v2/aggs/ticker/{nombre_ticker}/range/1/day/2023-01-01/2023-01-10?apiKey=_E06pfStxn1XpmSEBG7HUwEeV7029dfW")
        resultados_API=ticker_API_datos.json()
        return resultados_API['queryCount']
    
    def verificacion_datos(self, nombre_ticker, fecha_inicio, fecha_final):
        lista_ticker_guardados, df =self.ticker_db.tabla_principal_buscar()
        try:    #SI EL TICKER ES NUEVO Y NO EXISTE EN NUESTRA BASE DE DATOS
                indice_ticker=lista_ticker_guardados.index(nombre_ticker)
        except ValueError: #SI EL TICKER EXISTE EN NUESTRA BASE DE DATOS, VERIFICAMOS LAS FECHAS 
                data=self.solicitar_datos_ticker(nombre_ticker,fecha_inicio,fecha_final)
                self.ticker_db.tabla_ticker_crear(data,nombre_ticker)
                self.ticker_db.tabla_principal_insertar(nombre_ticker,fecha_inicio,fecha_final)
        else:                
                fecha_inicio_BD=datetime.strptime(df.FechaInicio[indice_ticker],self.format).date() #Fecha Inicial Base de Datos
                fecha_final_BD=datetime.strptime(df.FechaFinal[indice_ticker],self.format).date() #Fecha final Base Datos
                DeltaDia=timedelta(1)

                if fecha_inicio<fecha_inicio_BD and fecha_inicio_BD<fecha_final<fecha_final_BD:
                        fecha_inicio_solicitud = fecha_inicio
                        fecha_final_solicitud = (fecha_inicio_BD-DeltaDia)
                        data=self.solicitar_datos_ticker(nombre_ticker,fecha_inicio_solicitud,fecha_final_solicitud)
                        self.ticker_db.tabla_ticker_insertar(data,nombre_ticker)
                        self.ticker_db.tabla_principal_actuaizar(nombre_ticker,fecha_inicio_solicitud,fecha_final_BD)

                elif fecha_inicio_BD<fecha_inicio<fecha_final_BD and fecha_final_BD<fecha_final:
                        fecha_inicio_solicitud = fecha_final_BD+DeltaDia
                        fecha_final_solicitud = fecha_final
                        data=self.solicitar_datos_ticker(nombre_ticker,fecha_inicio_solicitud,fecha_final_solicitud)
                        self.ticker_db.tabla_ticker_insertar(data,nombre_ticker)
                        self.ticker_db.tabla_principal_actuaizar(nombre_ticker,fecha_inicio_BD,fecha_final)

                elif (fecha_inicio<fecha_inicio_BD and fecha_final>fecha_final_BD) or (fecha_inicio<fecha_inicio_BD and 
                            fecha_final<fecha_final_BD)or(fecha_inicio>fecha_inicio_BD and fecha_final>fecha_final_BD):
                        fecha_inicio_solicitud=fecha_inicio
                        fecha_final_solicitud=fecha_final
                        data=self.solicitar_datos_ticker(nombre_ticker,fecha_inicio_solicitud,fecha_final_solicitud)
                        self.ticker_db.tabla_ticker_borrar(nombre_ticker)
                        self.ticker_db.tabla_principal_borrar_registro(nombre_ticker)
                        self.ticker_db.tabla_ticker_crear(data,nombre_ticker)
                        self.ticker_db.tabla_principal_insertar(nombre_ticker,fecha_inicio, fecha_final)
                        
                elif fecha_inicio>fecha_inicio_BD and fecha_final<fecha_final_BD:
                        print("\t\tESTOS DATOS YA ESTÁN DISPONIBLE EN NUESTRA BASE DE DATOS")

class validacion_datos:

    def __init__(self, ticker_data, ticker_db):
        self.ticker_data = ticker_data
        self.ticker_db = ticker_db
        self.format="%Y-%m-%d"
        self.nombre_ticker= " "
        
    def validar_ticker(self):
        lista_ticker_guardados=self.ticker_db.tabla_principal_buscar()
        while True:
                try:
                    self.nombre_ticker=input("\t\tINGRESE EL TICKER: ").upper()  
                    lista_ticker_guardados.index(self.nombre_ticker) #PRIMERO VERIFICA SI EL TICKER EXISTE EN NUESTRA BASE DE DATOS
                except ValueError:             #SI NO EXISTE, ENTONCES LA VA A VALIDAR HACIENDO UN REQUEST
                    query_count=self.ticker_data.validar_ticker(self.nombre_ticker)     
                    if query_count!=0: #si el querycount es cero, significa que el tiker no existe.
                        break
                    else: 
                        print("\t\tEL TICKER INGRESADO ES INVÁLIDO\n") #SI EL TICKER NO EXISTE DEBERÍA IMPRIMIR UN ERROR.
                    continue
                else:
                    break
        return self.nombre_ticker

    def validar_fechas(self):
        while True:
            while True:             #Valida Fecha Inicial
                try:
                    fecha_inicio_str=input('\t\tINGRESE FECHA INICIAL (YYYY-MM-DD):')        
                    fecha_inicio=datetime.strptime(fecha_inicio_str,self.format).date() 
                except ValueError:
                    print("\t\tLA FECHA INICIAL INGRESADA ES INVÁLIDA!\n")
                else:
                    break
            while True:             #Valida Fecha Final con la misma lógica que la Fecha Inicial
                try:
                    fecha_final_str=input('\t\tINGRESE FECHA FINAL (YYYY-MM-DD):')
                    fecha_final=datetime.strptime(fecha_final_str,self.format).date()      
                except ValueError:
                    print("\t\tLA FECHA FINAL INGRESADA ES INVÁLIDA!\n")
                else:
                    break
            #Acá, lo que hace es que compara la fechas ingresadas en formato Date(), y nos marca los errores
            if fecha_inicio>fecha_final:
                print("\n\t\tERROR! LA FECHA INICIAL ES POSTERIOR A LA FECHA FINAL") 
            elif fecha_inicio>datetime.now().date() or fecha_final>datetime.now().date():
                print(f"\n\t\tERROR! UNA DE LAS FECHAS INGRESADAS ES POSTERIOR A LA FECHA ACTUAL {datetime.now().date()}\n")
            else: 
                break
        #Si da todo bien, retorna las fechas ingresadas en formato date(). 
        return fecha_inicio, fecha_final

class sub_menus:
        
    def __init__(self, ticker_db):
        self.ticker_db = ticker_db
    
    def visualizar(self):
        while True: #SELECCIÓN DE MENÚ
                print("\nVISUALIZACIÓN DE DATOS\n")
                print("\tSELECCIONE UNA OPCIÓN:\n\n\t\t1.RESUMEN\n\t\t2.GRÁFICO DE TICKER\n\t\t3.VOLVER")
                opcion=input("\n\t\tINGRESE OPCIÓN: ")   
                if opcion == "1":
                            system("cls")
                            self.ticker_db.tabla_princal_visualizar()
                            self.consultaFinal()
                            break
                elif opcion=="2": 
                            system("cls")
                            self.graficarTicker()
                            self.consultaFinal()
                            break
                elif opcion=="3":   
                            main()
                            break
                else:   
                    print("\t\tOPCIÓN INVÁLIDA")    
        
    def graficarTicker(self):
        print("GRAFICO DE DATOS\n")
        listaTicker, df= self.ticker_db.tabla_principal_buscar()
        list(df.Ticker)
        while True:
            try:
                tickerPrint=input("\tINGRESE EL TICKER A GRAFICAR: ").upper()  
                listaTicker.index(tickerPrint)
                break #PRIMERO VERIFICA SI EL TICKER EXISTE EN NUESTRA BASE DE DATOS
            except ValueError: 
                print("\tEL TICKER INGRESADO NO ESTÁ EN NUESTRA BASE DE DATOS, SOLICITELA DESDE EL MENÚ PRINCIPAL.\n")
                continue

        #buscamos las tablas del ticker correspondiente
        dPrint=self.ticker_db.tabla_ticker_buscar(tickerPrint)    
        print("\n\tLOS PARÁMETROS DISPONIBLES SON:")
        print("\n\t\t1.VOLUMEN OPERADO\n\t\t2.PRECIO PROMEDIO POR VOLUMEN\n\t\t3.PRECIO APERTURA")
        print("\t\t4.PRECIO CIERRE\n\t\t5.PRECIO MÁS ALTO\n\t\t6.PRECIO MÁS BAJO\n\t\t7.NÚMERO DE TRANSACCIONES ")
        while True:
                parametro=input(f"\n\t\tQUE PARÁMETRO DESEA IMPRIMIR? (1-7):")
                if parametro=="1":
                        parametroPrint="VolumenOperado"
                        break
                elif parametro=="2":
                        parametroPrint="PrecioPromedioPorVolumen"
                        break
                elif parametro=="3":
                        parametroPrint="PrecioApertura"
                        break
                elif parametro=="4":
                        parametroPrint="PrecioCierre"
                        break
                elif parametro=="5":
                        parametroPrint="PrecioMásAlto"
                        break
                elif parametro=="6":
                        parametroPrint="PrecioMásBajo"
                        break
                elif parametro=="7":    
                        parametroPrint="NúmeroDeTransacciones"
                        break
                else: 
                        print("\t\tERROR!! PARÁMETRO INEXISTENTE")
                        continue
             
        dPrint.plot("Fecha",f"{parametroPrint}",kind="line",title=f"{tickerPrint}")
        plt.show()



    def consultaFinal(self):
                while True:
                    opc=input("\n\tDESEA REGRESAR AL MENÚ PRINCIPAL (Y/N)? :").upper()
                    if opc=="Y":
                            system("cls")
                            main()                  
                    elif opc=="N":
                            print("\n\tHASTA LUEGO..")
                            sys.exit()
                    else:
                            print("OPCIÓN INVÁLIDA)")
                            continue

def main():
    
    #Creamos las instancias para los objetos
    ticker_db=ticker_base_datos()  #Creamos la base de datos en caso de que no exista
    ticker_db.tabla_principal_crear()
    sub_menu=sub_menus(ticker_db)
    ticker_actualizado=ticker_actualizar(ticker_db, sub_menu)
    datos_validos=validacion_datos(ticker_actualizado,ticker_db)
    

    print("\nITBA - TRABAJO FINAL DE PYTHON - INFORMACIÓN DE TICKER ")
    while True: #SELECCIÓN DE MENÚ
        print("\n\tSELECCIONE UNA OPCIÓN:\n\n\t\t1. ACTUALIZACIÓN DE DATOS\n\t\t2. VISUALIZACIÓN DE DATOS\n\t\t3. SALIR")
        opcion=input("\n\t\tINGRESE OPCIÓN: ")   
        if opcion == "1":
            system("cls")
            print("\n\tACTUALIZACIÓN DE DATOS\n")
            nombre_ticker=datos_validos.validar_ticker()
            fecha_inicio, fecha_final=datos_validos.validar_fechas()
            ticker_actualizado.verificacion_datos(nombre_ticker, fecha_inicio, fecha_final)
            sub_menu.consultaFinal()
            break
        elif opcion=="2": 
            system("cls")
            sub_menu.visualizar()
            break
        elif opcion=="3":   
            print("\t\n\t\t\tHASTA LUEGO..")
            sys.exit()
        else:   
            print("\tOPCIÓN INVÁLIDA")

    ticker_db.cerrar_bd() #Cierra conexión con base de datos

main()