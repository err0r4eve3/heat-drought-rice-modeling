// GEE export: province_daily_climate_2000_2024.csv
// Purpose: generate province × date table with tmax_c and precipitation_mm.
// Data sources:
// - ERA5-Land Daily Aggregated: ECMWF/ERA5_LAND/DAILY_AGGR
// - CHIRPS Daily: UCSB-CHG/CHIRPS/DAILY
//
// Before running:
// 1) Upload a 2022/current China province boundary asset to GEE.
// 2) Make sure each province feature has province name and province code fields.
// 3) Run in 5-year chunks to avoid export timeout.
// 4) After each chunk finishes, concatenate CSVs locally and place as:
//    data/interim/province_daily_climate_2000_2024.csv

// ===== User parameters =====
var PROVINCE_ASSET = 'users/YOUR_USERNAME/china_provinces_2022';  // replace
var PROVINCE_NAME_FIELD = 'province';       // replace if your asset uses NAME_1 / name / 省份
var PROVINCE_CODE_FIELD = 'province_code';  // replace if your asset uses adcode / code
var CHUNK_START_YEAR = 2000;
var CHUNK_END_YEAR = 2004;  // inclusive; run 2000-2004, 2005-2009, ..., 2020-2024
var EXPORT_FOLDER = 'heat_drought_rice_modeling';
var EXPORT_PREFIX = 'province_daily_climate';

// ===== Boundary preparation =====
var provincesRaw = ee.FeatureCollection(PROVINCE_ASSET);
var provinces = provincesRaw.map(function (f) {
  return ee.Feature(f.geometry(), {
    province: f.get(PROVINCE_NAME_FIELD),
    province_code: f.get(PROVINCE_CODE_FIELD)
  });
});

// ===== Dataset preparation =====
var start = ee.Date.fromYMD(CHUNK_START_YEAR, 1, 1);
var end = ee.Date.fromYMD(CHUNK_END_YEAR + 1, 1, 1);

var era5 = ee.ImageCollection('ECMWF/ERA5_LAND/DAILY_AGGR')
  .filterDate(start, end)
  .select('temperature_2m_max');

var chirps = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY')
  .filterDate(start, end)
  .select('precipitation');

// Build one FeatureCollection per day, then flatten.
var nDays = end.difference(start, 'day');
var days = ee.List.sequence(0, nDays.subtract(1));

var dailyFc = ee.FeatureCollection(days.map(function (d) {
  var date = start.advance(ee.Number(d), 'day');
  var nextDate = date.advance(1, 'day');

  var tmax = ee.Image(era5.filterDate(date, nextDate).first())
    .select('temperature_2m_max')
    .subtract(273.15)
    .rename('tmax_c');

  var precip = ee.Image(chirps.filterDate(date, nextDate).first())
    .select('precipitation')
    .rename('precipitation_mm');

  var img = ee.Image.cat([tmax, precip]);

  var reduced = img.reduceRegions({
    collection: provinces,
    reducer: ee.Reducer.mean(),
    scale: 10000,
    crs: 'EPSG:4326',
    tileScale: 4
  }).map(function (f) {
    return f.set({
      date: date.format('YYYY-MM-dd'),
      year: date.get('year'),
      month: date.get('month')
    }).select([
      'province', 'province_code', 'date', 'year', 'month',
      'tmax_c', 'precipitation_mm'
    ]);
  });

  return reduced;
})).flatten();

print('Preview', dailyFc.limit(10));
print('Expected rows approx', provinces.size().multiply(nDays));

Export.table.toDrive({
  collection: dailyFc,
  description: EXPORT_PREFIX + '_' + CHUNK_START_YEAR + '_' + CHUNK_END_YEAR,
  folder: EXPORT_FOLDER,
  fileNamePrefix: EXPORT_PREFIX + '_' + CHUNK_START_YEAR + '_' + CHUNK_END_YEAR,
  fileFormat: 'CSV',
  selectors: [
    'province', 'province_code', 'date', 'year', 'month',
    'tmax_c', 'precipitation_mm'
  ]
});
