import ee
import geemap
import datetime
import json
import os
import pandas as pd

# Autenticación
credentials_json = os.environ['GEE_CREDENTIALS']
credentials_dict = json.loads(credentials_json)
credentials = ee.ServiceAccountCredentials(
    email=credentials_dict['client_email'],
    key_data=credentials_dict['private_key']
)
ee.Initialize(credentials)

year = datetime.datetime.now().year
year_base = year - 6

# AGEBs
agebs = ee.FeatureCollection('projects/ee-practicas-satelites/assets/zmvm_final_v2')

# Funciones
def ndvi_anual(year, region):
    return (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterBounds(region)
        .filter(ee.Filter.calendarRange(year, year, 'year'))
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
        .map(lambda img: img.normalizedDifference(['B8', 'B4']).rename('NDVI'))
        .median())

def ndbi_anual(year, region):
    return (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterBounds(region)
        .filter(ee.Filter.calendarRange(year, year, 'year'))
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
        .map(lambda img: img.normalizedDifference(['B11', 'B8']).rename('NDBI'))
        .median())

# Calcular NDVI actual y base
ndvi_actual = geemap.ee_to_df(ndvi_anual(year, agebs).reduceRegions(
    collection=agebs, reducer=ee.Reducer.mean(), scale=100
).select(['CVEGEO', 'mean']))
ndvi_actual.rename(columns={'mean': f'ndvi_{year}'}, inplace=True)

ndvi_base = geemap.ee_to_df(ndvi_anual(year_base, agebs).reduceRegions(
    collection=agebs, reducer=ee.Reducer.mean(), scale=100
).select(['CVEGEO', 'mean']))
ndvi_base.rename(columns={'mean': f'ndvi_{year_base}'}, inplace=True)

# Calcular NDBI actual
ndbi_actual = geemap.ee_to_df(ndbi_anual(year, agebs).reduceRegions(
    collection=agebs, reducer=ee.Reducer.mean(), scale=100
).select(['CVEGEO', 'mean']))
ndbi_actual.rename(columns={'mean': f'ndbi_{year}'}, inplace=True)

# Merge y delta
df = ndvi_actual.merge(ndvi_base, on='CVEGEO').merge(ndbi_actual, on='CVEGEO')
df['delta_ndvi'] = df[f'ndvi_{year}'] - df[f'ndvi_{year_base}']

# Guardar
os.makedirs('data', exist_ok=True)
df.to_csv(f'data/satelital_{year}.csv', index=False)
print(f'satelital_{year}.csv guardado')