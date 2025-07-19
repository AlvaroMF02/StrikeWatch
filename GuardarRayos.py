import mysql.connector
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import reverse_geocoder as rg
import json
import aiohttp
import asyncio

# ʕ•́ᴥ•̀ʔっ
# Todos los datos han sido recogidos de la web https://map.blitzortung.org/

# Función para hacer la descompresión de los datos que vienen de la web
def descomprimirDatosWeb(compressed_str):
    compressed = [ord(c) for c in compressed_str]
    dict_size = 256
    dictionary = {i: chr(i) for i in range(dict_size)}

    result = []
    w = chr(compressed.pop(0))
    result.append(w)

    for k in compressed:
        if k in dictionary:
            entry = dictionary[k]
        elif k == dict_size:
            entry = w + w[0]
        else:
            raise ValueError(f"Bad compressed k: {k}")
        result.append(entry)

        dictionary[dict_size] = w + entry[0]
        dict_size += 1

        w = entry

    return "".join(result)


# Función para guardar Pais por latitud y longitud
def obtenerPais(lat, lon):
    try:
        pais = rg.search((lat, lon))
        return pais[0]['name']
    except:
        return 'Desconocido'



# Función para limpiar todos los datos y agregar nuevos (fecha, hora, pais), también arregla estaciones
def limpiarDatos(d):
    
    timeNs = d.get("time")

    try:
        # Convertir nanosegundos a segundos
        timeSeg = int(timeNs) / 1e9
        dt = datetime.fromtimestamp(timeSeg, tz=timezone.utc)
        horaEsp = dt.astimezone(ZoneInfo("Europe/Madrid"))
        
        fecha = dt.strftime("%Y-%m-%d")
        hora = horaEsp.strftime("%H:%M:%S")
        
    except Exception as e:
        print("⚠️ Error al convertir timestamp:", timeNs, e)
        fecha = "1970-01-01"
        hora = "00:00:00"
    
    pais = obtenerPais(d.get("lat"),d.get("lon"))


    datosLimpios = {
        "time": int(timeNs),
        "lat": d.get("lat"),
        "lon": d.get("lon"),
        "alt": d.get("alt"),
        "pol": d.get("pol"),
        "mds": d.get("mds"),
        "mcg": d.get("mcg"),
        "status": d.get("status"),
        "region": d.get("region"),
        "delay": d.get("delay"),
        "lonc": d.get("lonc"),
        "latc": d.get("latc"),
        "fecha": fecha,
        "hora": hora,
        "pais": pais,
        "estaciones": d.get("sig", [])
    }

    return datosLimpios


# Función para insertar los datos al servidos SQL, inserta en rayos y estaciones
def insertarRayo(data):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="rayos"
        )
        
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO rayos (time, lat, lon, alt, pol, mds, mcg, status, region, delay, lonc, latc, fecha, hora, pais)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get("time"),
            data.get("lat"),
            data.get("lon"),
            data.get("alt"),
            data.get("pol"),
            data.get("mds"),
            data.get("mcg"),
            data.get("status"),
            data.get("region"),
            data.get("delay"),
            data.get("lonc"),
            data.get("latc"),
            data.get("fecha"),
            data.get("hora"),
            data.get("pais"),
        ))
        
        idRayo = cursor.lastrowid
        
        # Insertar hasta 5 estaciones asociadas a este rayo
        for cont, estacion in enumerate(data.get("estaciones", [])):
            if cont >= 5:
                break  # solo guardar 5 estaciones como máximo
            cursor.execute("""
                INSERT INTO estaciones (idRayo, sta, tiempoRelat, lat, lon, alt, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                idRayo,
                estacion.get("sta"),
                estacion.get("time"),
                estacion.get("lat"),
                estacion.get("lon"),
                estacion.get("alt"),
                estacion.get("status")
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        print("❌ Error al guardar en MySQL:", e)


# Función principal en la que se hace la conexión a la web y se recogen los datos
async def escuchar():
    url = "wss://ws2.blitzortung.org/"
    headers = {
        "Origin": "https://www.blitzortung.org",
        "User-Agent": "Mozilla/5.0",
    }

    session = aiohttp.ClientSession()
    contRayos = 0

    try:
        async with session.ws_connect(url, headers=headers) as ws:
            await ws.send_str('{"a":111}')  # handshake inicial

            async for msj in ws:
                if msj.type == aiohttp.WSMsgType.TEXT:
                    
                    try:
                        raw = msj.data
                        # Descompresión
                        texto = descomprimirDatosWeb(raw)
                        data = json.loads(texto)
                        # Limpieza
                        rayoLimp = limpiarDatos(data)
                        
                        if rayoLimp is not None:
                            # Inserción
                            insertarRayo(rayoLimp)
                            contRayos += 1
                            print(f"✅ Rayo {contRayos} guardado")
                        
                        # contador
                        if contRayos >= 10:
                            print("✅ Se han procesado 10 rayos.")
                            break
                        
                    except Exception as e:
                        print("❌ Error al procesar mensaje:", e)
                        
                elif msj.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                    print("❌ WebSocket cerrado o con error")
                    break
    finally:
        await session.close()


# MAIN
async def main():
    logging.info("Iniciando conexión con el WebSocket en blitzortung.org...")
    await escuchar()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("Script interrumpido manualmente")
    except Exception as e:
        logging.error(f"Error inesperado: {e}")