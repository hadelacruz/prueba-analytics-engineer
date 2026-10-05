# Databricks notebook source
import os
import sys

CATALOG = "workspace"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

LANDING_PATH = f"/Volumes/{CATALOG}/bronze/landing"
EXPORTS_PATH = f"/Volumes/{CATALOG}/gold/exports"

# Raíz del repo, para importar src.banguat
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if not os.path.isdir(os.path.join(REPO_ROOT, "src")):
    raise RuntimeError(f"No se encontró la carpeta src en {REPO_ROOT}")
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
