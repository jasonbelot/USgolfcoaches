import json
import math
import os
import re
from typing import Dict, Tuple, Optional, List

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import folium
from streamlit_folium import st_folium

# ----------------------------
# Config
# ----------------------------
st.set_page_config(page_title="NCAA D1 Golf - Map & Coach Finder", layout="wide")

DATA_DIR = "data"
XLSM_PATH = os.path.join(DATA_DIR, "Recensement coachs US.xlsm")
CACHE_PATH = os.path.join(DATA_DIR, "geocode_cache.json")

MEN_D1_URL = "https://juniorgolfhub.com/mens-ncaa-division-1-golf-schools"
WOMEN_D1_URL = "https://juniorgolfhub.com/womens-ncaa-division-1-golf-schools"

# État -> abréviation (pour aider le géocodage)
STATE_ABBR = {
    "ALABAMA": "AL","ALASKA":"AK","ARIZONA":"AZ","ARKANSAS":"AR","CALIFORNIA":"CA","COLORADO":"CO",
    "CONNECTICUT":"CT","DELAWARE":"DE","FLORIDA":"FL","GEORGIA":"GA","HAWAII":"HI","IDAHO":"ID",
    "ILLINOIS":"IL","INDIANA":"IN","IOWA":"IA","KANSAS":"KS","KENTUCKY":"KY","LOUISIANA":"LA",
    "MAINE":"ME","MARYLAND":"MD","MASSACHUSETTS":"MA","MICHIGAN":"MI","MINNESOTA":"MN",
    "MISSISSIPPI":"MS","MISSOURI":"MO","MONTANA":"MT","NEBRASKA":"NE","NEVADA":"NV",
    "NEW HAMPSHIRE":"NH","NEW JERSEY":"NJ","NEW MEXICO":"NM","NEW YORK":"NY","NORTH CAROLINA":"NC",
    "NORTH DAKOTA":"ND","OHIO":"OH","OKLAHOMA":"OK","OREGON":"OR","PENNSYLVANIA":"PA",
    "RHODE ISLAND":"RI","SOUTH CAROLINA":"SC","SOUTH DAKOTA":"SD","TENNESSEE":"TN","TEXAS":"TX",
    "UTAH":"UT","VERMONT":"VT","VIRGINIA":"VA","WASHINGTON":"WA","WEST VIRGINIA":"WV",
    "WISCONSIN":"WI","WYOMING":"WY","DISTRICT OF COLUMBIA":"DC"
}

# ----------------------------
# Utils
# ----------------------------
def load_cache(path: str) -> Dict[str, Dict]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(path: str, cache: Dict[str, Dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = a
    lat2, lon2 = b
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(x))

def clean_text(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()

def parse_city_from_golf(golf_field: str) -> str:
    """
    Dans ton fichier, 'Golf' ressemble souvent à:
      'Nom du club, Birmingham'
    On récupère la partie après la dernière virgule comme ville.
    """
    s = clean_text(golf_field)
    if "," in s:
        return clean_text(s.split(",")[-1])
    return s

@st.cache_resource
def get_geocoder():
    geolocator = Nominatim(user_agent="ncaa_golf_streamlit_app")
    # rate limiter (respect Nominatim)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1, swallow_exceptions=True)
    return geocode

def geocode_with_cache(query: str, cache: Dict[str, Dict]) -> Optional[Tuple[float, float]]:
    q = clean_text(query)
    if not q:
        return None

    if q in cache and "lat" in cache[q] and "lon" in cache[q]:
        return (cache[q]["lat"], cache[q]["lon"])

    geocode = get_geocoder()
    loc = geocode(q)
    if loc is None:
        cache[q] = {"ok": False}
        return None

    cache[q] = {"ok": True, "lat": float(loc.latitude), "lon": float(loc.longitude)}
    return (float(loc.latitude), float(loc.longitude))

def scrape_d1_universities(url: str) -> List[str]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Sur ces pages, les écoles sont listées en texte (souvent sous forme de liste/paragraphes).
    # On récupère tous les <li> et on filtre.
    candidates = []
    for li in soup.select("li"):
        t = clean_text(li.get_text(" ", strip=True))
        if len(t) >= 4 and "NCAA" not in t and "Division" not in t:
            candidates.append(t)

    # fallback si la page change
    if len(candidates) < 50:
        text = clean_text(soup.get_text(" ", strip=True))
        # extraction naïve : lignes avec "University" / "College" / "State" etc.
        raw = re.split(r"\s{2,}|\n", text)
        for x in raw:
            t = clean_text(x)
            if any(k in t for k in ["University", "College", "State", "Institute", "Academy"]):
                if 4 <= len(t) <= 80:
                    candidates.append(t)

    # dédoublonnage
    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out

@st.cache_data(ttl=24*3600)
def load_university_list(include_men: bool, include_women: bool) -> List[str]:
    schools = []
    if include_men:
        schools += scrape_d1_universities(MEN_D1_URL)
    if include_women:
        schools += scrape_d1_universities(WOMEN_D1_URL)

    # dédoublonnage + nettoyage
    out = []
    seen = set()
    for s in schools:
        s2 = clean_text(s)
        if s2 and s2 not in seen:
            seen.add(s2)
            out.append(s2)
    return out

def load_coaches(xlsm_path: str) -> pd.DataFrame:
    df = pd.read_excel(xlsm_path, sheet_name="TableGolf", engine="openpyxl")
    df = df.rename(columns={c: c.strip() for c in df.columns})
    df["Etat"] = df["Etat"].astype(str).str.strip().str.upper()
    df["Coach"] = df["Coach"].astype(str).map(clean_text)
    df["Golf"] = df["Golf"].astype(str).map(clean_text)
    if "Phone" in df.columns:
        df["Phone"] = df["Phone"].astype(str).map(clean_text)
        df.loc[df["Phone"].str.lower().isin(["nan", "none"]), "Phone"] = ""
    else:
        df["Phone"] = ""
    df["City"] = df["Golf"].map(parse_city_from_golf)
    df["StateAbbr"] = df["Etat"].map(lambda x: STATE_ABBR.get(x, x))
    return df

# ----------------------------
# UI
# ----------------------------
st.title("Carte NCAA Division I (Golf) + Coachs triés par distance")

if not os.path.exists(XLSM_PATH):
    st.error(f"Fichier introuvable: {XLSM_PATH}\n\n➡️ Mets 'Recensement coachs US.xlsm' dans le dossier data/")
    st.stop()

with st.sidebar:
    st.header("Sources & options")
    include_men = st.checkbox("Inclure NCAA D1 Men", value=True)
    include_women = st.checkbox("Inclure NCAA D1 Women", value=True)
    max_coaches = st.slider("Nombre de coachs affichés (au clic)", 10, 200, 50, 10)
    unit = st.radio("Unité distance", ["km", "miles"], index=0)
    st.caption("Le géocodage utilise Nominatim (OpenStreetMap) et un cache local pour limiter les requêtes.")

cache = load_cache(CACHE_PATH)

# Data
coaches_df = load_coaches(XLSM_PATH)

# Geocode coach locations (par City + State)
st.subheader("Préparation des coordonnées (cache local)")
with st.expander("Voir l'état du géocodage", expanded=False):
    st.write("On géocode les coachs par (Ville, État) pour limiter les appels.")

unique_places = coaches_df[["City", "StateAbbr"]].drop_duplicates()
missing = 0
coords_map = {}  # (City,State) -> (lat,lon)
progress = st.progress(0)

for i, row in enumerate(unique_places.itertuples(index=False), start=1):
    city, st_abbr = row.City, row.StateAbbr
    query = f"{city}, {st_abbr}, USA"
    coord = geocode_with_cache(query, cache)
    if coord is None:
        missing += 1
    else:
        coords_map[(city, st_abbr)] = coord
    progress.progress(i / len(unique_places))

save_cache(CACHE_PATH, cache)
st.caption(f"✅ Coachs: {len(coaches_df)} lignes | Lieux uniques: {len(unique_places)} | Non géocodés: {missing}")

# Universities
st.subheader("Universités NCAA D1")
schools = load_university_list(include_men=include_men, include_women=include_women)

st.write(f"Universités récupérées (liste web): **{len(schools)}**")
search = st.text_input("Filtrer universités (optionnel)", value="")
if search.strip():
    schools_filtered = [s for s in schools if search.lower() in s.lower()]
else:
    schools_filtered = schools

# Geocode universities (limité pour UX)
limit = st.slider("Nombre d'universités à afficher sur la carte", 50, min(400, len(schools_filtered)), min(150, len(schools_filtered)), 25)
schools_to_map = schools_filtered[:limit]

uni_rows = []
progress2 = st.progress(0)
for i, name in enumerate(schools_to_map, start=1):
    q = f"{name}, USA"
    coord = geocode_with_cache(q, cache)
    if coord is not None:
        uni_rows.append({"University": name, "lat": coord[0], "lon": coord[1]})
    progress2.progress(i / max(1, len(schools_to_map)))

save_cache(CACHE_PATH, cache)

uni_df = pd.DataFrame(uni_rows)
if uni_df.empty:
    st.error("Aucune université géocodée. (La source web a peut-être changé, ou le géocodage est bloqué.)")
    st.stop()

# Map
st.subheader("Carte interactive")
center_lat, center_lon = float(uni_df["lat"].mean()), float(uni_df["lon"].mean())
m = folium.Map(location=[center_lat, center_lon], zoom_start=4, tiles="CartoDB positron")

for r in uni_df.itertuples(index=False):
    folium.Marker(
        location=[r.lat, r.lon],
        tooltip=r.University,
        popup=r.University
    ).add_to(m)

map_state = st_folium(m, height=600, width=None)

# Click handling
clicked = None
if map_state and map_state.get("last_object_clicked_popup"):
    clicked = map_state["last_object_clicked_popup"]

st.subheader("Résultats (clic sur une université)")
if not clicked:
    st.info("Clique sur un marqueur d'université pour afficher les coachs triés par distance.")
else:
    st.success(f"Université sélectionnée : **{clicked}**")

    # coords université
    uni = uni_df[uni_df["University"] == clicked]
    if uni.empty:
        st.warning("Coordonnées université introuvables dans la table courante (peut arriver si doublon/filtre).")
    else:
        uni_coord = (float(uni.iloc[0]["lat"]), float(uni.iloc[0]["lon"]))

        # calcul distance aux coachs (si coord coach connue)
        rows = []
        for rr in coaches_df.itertuples(index=False):
            c_key = (rr.City, rr.StateAbbr)
            c_coord = coords_map.get(c_key)
            if c_coord is None:
                continue
            d_km = haversine_km(uni_coord, c_coord)
            rows.append({
                "Coach": rr.Coach,
                "Golf": rr.Golf,
                "Phone": rr.Phone,
                "Etat": rr.Etat,
                "Ville": rr.City,
                "Distance_km": d_km
            })

        out = pd.DataFrame(rows).sort_values("Distance_km", ascending=True).head(max_coaches)

        if out.empty:
            st.warning("Aucun coach géocodé disponible pour calculer la distance.")
        else:
            if unit == "miles":
                out["Distance_miles"] = out["Distance_km"] * 0.621371
                out = out.drop(columns=["Distance_km"])
            else:
                out["Distance_km"] = out["Distance_km"].round(2)

            st.dataframe(out, use_container_width=True)
