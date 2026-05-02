// Export province-level daily tmax and precipitation for 2000-2024.
//
// Before running:
// 1. Replace PROVINCE_ASSET with a province boundary FeatureCollection asset.
// 2. Ensure each province feature has province and province_code properties.
// 3. Export the CSV to Drive, then place it at:
//    data/interim/province_daily_climate_2000_2024.csv

var PROVINCE_ASSET = 'users/your_account/china_provinces_2022';
var START_DATE = '2000-01-01';
var END_DATE = '2025-01-01';
var EXPORT_DESCRIPTION = 'province_daily_climate_2000_2024';
var EXPORT_FOLDER = 'gee_exports';

var provinces = ee.FeatureCollection(PROVINCE_ASSET);

// ERA5-Land Daily Aggregated covers the full 2000-2024 target window in GEE.
// temperature_2m_max is Kelvin and is converted to Celsius below.
var tmax = ee.ImageCollection('ECMWF/ERA5_LAND/DAILY_AGGR')
  .filterDate(START_DATE, END_DATE)
  .select('temperature_2m_max')
  .map(function(image) {
    return image
      .subtract(273.15)
      .rename('tmax_c')
      .copyProperties(image, ['system:time_start']);
  });

var precip = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY')
  .filterDate(START_DATE, END_DATE)
  .select('precipitation')
  .map(function(image) {
    return image
      .rename('precipitation_mm')
      .copyProperties(image, ['system:time_start']);
  });

var joined = ee.ImageCollection(
  ee.Join.inner().apply({
    primary: tmax,
    secondary: precip,
    condition: ee.Filter.equals({
      leftField: 'system:time_start',
      rightField: 'system:time_start'
    })
  }).map(function(pair) {
    var left = ee.Image(pair.get('primary'));
    var right = ee.Image(pair.get('secondary'));
    return left.addBands(right).copyProperties(left, ['system:time_start']);
  })
);

var dailyRows = joined.map(function(image) {
  var date = ee.Date(image.get('system:time_start'));
  var reduced = image.reduceRegions({
    collection: provinces,
    reducer: ee.Reducer.mean(),
    scale: 10000,
    crs: 'EPSG:4326'
  });
  return reduced.map(function(feature) {
    return ee.Feature(null, {
      province: feature.get('province'),
      province_code: feature.get('province_code'),
      date: date.format('YYYY-MM-dd'),
      year: date.get('year'),
      month: date.get('month'),
      tmax_c: feature.get('tmax_c'),
      precipitation_mm: feature.get('precipitation_mm')
    });
  });
}).flatten();

Export.table.toDrive({
  collection: dailyRows,
  description: EXPORT_DESCRIPTION,
  folder: EXPORT_FOLDER,
  fileNamePrefix: EXPORT_DESCRIPTION,
  fileFormat: 'CSV',
  selectors: [
    'province',
    'province_code',
    'date',
    'year',
    'month',
    'tmax_c',
    'precipitation_mm'
  ]
});
