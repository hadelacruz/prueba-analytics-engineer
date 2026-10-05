"""Cliente del servicio SOAP de tipo de cambio del Banco de Guatemala (Banguat).

Usa la operación TipoCambioRango(fechainit, fechafin), con fechas en formato dd/mm/yyyy.
"""
from __future__ import annotations

import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

import pandas as pd
import requests

URL = "https://www.banguat.gob.gt/variables/ws/TipoCambio.asmx"
NS_WS = "http://www.banguat.gob.gt/variables/ws/"
NS_SOAP = "http://schemas.xmlsoap.org/soap/envelope/"
SOAP_ACTION = f'"{NS_WS}TipoCambioRango"'
MONEDA_USD = "2"

log = logging.getLogger(__name__)


class BanguatError(Exception):
    """Error base del cliente."""


class ServicioNoDisponibleError(BanguatError):
    """Timeout, error de conexión o 5xx después de agotar los reintentos."""


class RespuestaHTTPError(BanguatError):
    """Respuesta 4xx: no se reintenta."""

    def __init__(self, status: int, cuerpo: str):
        super().__init__(f"HTTP {status}: {cuerpo[:300]}")
        self.status = status


class XMLInvalidoError(BanguatError):
    """XML mal formado, SOAP Fault o estructura inesperada."""


class SinDatosError(BanguatError):
    """Respuesta válida pero sin tasas para el rango."""


class RespuestaInconsistenteError(BanguatError):
    """La respuesta trae fechas fuera del rango pedido."""


@dataclass
class RespuestaBanguat:
    xml_crudo: str
    fecha_consulta: datetime
    fecha_ini: date
    fecha_fin: date
    http_status: int
    intentos: int
    endpoint: str = URL


def armar_sobre_soap(fecha_ini: date, fecha_fin: date) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        f'<soap:Envelope xmlns:soap="{NS_SOAP}">'
        "<soap:Body>"
        f'<TipoCambioRango xmlns="{NS_WS}">'
        f"<fechainit>{fecha_ini:%d/%m/%Y}</fechainit>"
        f"<fechafin>{fecha_fin:%d/%m/%Y}</fechafin>"
        "</TipoCambioRango>"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def consultar_tipo_cambio_rango(
    fecha_ini: date,
    fecha_fin: date,
    *,
    url: str = URL,
    timeout: float = 30,
    max_intentos: int = 3,
    backoff_seg: float = 2.0,
    session: requests.Session | None = None,
) -> RespuestaBanguat:
    """Consulta todo el rango en una sola llamada y devuelve el XML crudo.

    Reintenta con backoff exponencial ante timeout, error de conexión, 429 y 5xx.
    """
    if fecha_ini > fecha_fin:
        raise ValueError(f"fecha_ini ({fecha_ini}) es posterior a fecha_fin ({fecha_fin})")

    cuerpo = armar_sobre_soap(fecha_ini, fecha_fin).encode("utf-8")
    headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": SOAP_ACTION}
    http = session or requests.Session()
    ultimo_error: Exception | None = None

    for intento in range(1, max_intentos + 1):
        try:
            resp = http.post(url, data=cuerpo, headers=headers, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            ultimo_error = e
            log.warning("Intento %s/%s falló: %s", intento, max_intentos, e)
        else:
            if resp.status_code == 200:
                return RespuestaBanguat(
                    xml_crudo=resp.content.decode("utf-8"),
                    fecha_consulta=datetime.now(timezone.utc),
                    fecha_ini=fecha_ini,
                    fecha_fin=fecha_fin,
                    http_status=resp.status_code,
                    intentos=intento,
                    endpoint=url,
                )
            error_http = RespuestaHTTPError(resp.status_code, resp.text)
            if resp.status_code < 500 and resp.status_code != 429:
                raise error_http
            ultimo_error = error_http
            log.warning("Intento %s/%s falló: %s", intento, max_intentos, error_http)

        if intento < max_intentos:
            time.sleep(backoff_seg * 2 ** (intento - 1))

    raise ServicioNoDisponibleError(
        f"Banguat no respondió correctamente después de {max_intentos} intentos: {ultimo_error}"
    ) from ultimo_error


def parsear_tipo_cambio(xml_crudo: str) -> pd.DataFrame:
    """Convierte la respuesta de TipoCambioRango en una tabla (fecha, tasa). La tasa es el valor de venta."""
    try:
        raiz = ET.fromstring(xml_crudo.encode("utf-8"))
    except ET.ParseError as e:
        raise XMLInvalidoError(f"La respuesta no es XML válido: {e}") from e

    fault = raiz.find(f".//{{{NS_SOAP}}}Fault")
    if fault is not None:
        raise XMLInvalidoError(f"SOAP Fault: {fault.findtext('faultstring', default='').strip()}")

    if raiz.find(f".//{{{NS_WS}}}TipoCambioRangoResult") is None:
        raise XMLInvalidoError("El XML no contiene TipoCambioRangoResult")

    filas = []
    for var in raiz.iter(f"{{{NS_WS}}}Var"):
        if var.findtext(f"{{{NS_WS}}}moneda", default="").strip() != MONEDA_USD:
            continue
        texto_fecha = var.findtext(f"{{{NS_WS}}}fecha", default="").strip()
        texto_tasa = var.findtext(f"{{{NS_WS}}}venta", default="").strip()
        try:
            fecha = datetime.strptime(texto_fecha, "%d/%m/%Y").date()
            tasa = Decimal(texto_tasa)
        except (ValueError, InvalidOperation) as e:
            raise XMLInvalidoError(f"Var con valores inesperados: fecha={texto_fecha!r}, venta={texto_tasa!r}") from e
        filas.append({"fecha": fecha, "tasa": tasa})

    if not filas:
        raise SinDatosError("La respuesta no trae tasas para el rango consultado")

    return pd.DataFrame(filas).drop_duplicates("fecha").sort_values("fecha").reset_index(drop=True)


def validar_rango(df: pd.DataFrame, fecha_ini: date, fecha_fin: date) -> list[date]:
    """Falla si hay fechas fuera del rango pedido y devuelve las fechas del rango que no tienen tasa."""
    fuera = df[(df["fecha"] < fecha_ini) | (df["fecha"] > fecha_fin)]
    if not fuera.empty:
        raise RespuestaInconsistenteError(
            f"{len(fuera)} fechas fuera del rango {fecha_ini}..{fecha_fin}, p. ej. {fuera['fecha'].iloc[0]}"
        )
    esperadas = {fecha_ini + timedelta(days=i) for i in range((fecha_fin - fecha_ini).days + 1)}
    return sorted(esperadas - set(df["fecha"]))


def obtener_tipo_cambio(fecha_ini: date, fecha_fin: date, **kwargs) -> pd.DataFrame:
    """Devuelve la tabla (fecha, tasa) del Banguat para el rango indicado."""
    respuesta = consultar_tipo_cambio_rango(fecha_ini, fecha_fin, **kwargs)
    df = parsear_tipo_cambio(respuesta.xml_crudo)
    validar_rango(df, fecha_ini, fecha_fin)
    return df
