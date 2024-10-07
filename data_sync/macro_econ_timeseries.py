from fredapi import Fred

API_KEY="8805f58d4d02b992a731e925ef01cd8e"
SERIES_TO_TRACK = [
    "UMCSENT", # consumer confidence index
    "MSPUS" # home sales
    "M2REAL", # money supply
    "IRLTLT01USM156N", # bond interest rate
    "MORTGAGE30US", # mortgage interest rate
    "DEXUSEU", # US->Euro exchange rates
    "DEXJPUS", # Us->Japan exchange rates
    "DEXCHUS", # Us->China exchange rates
    "DEXSIUS", # Us->Singapore exchange rates
    "PPIACO", # All commodities
    "WPU10", # metal commodities
    "WPS012", # grains commodities
    "DCOILWTICO", # oil commodities
]

fred = Fred(api_key=API_KEY)

df = fred.get_series_all_releases('DCOILWTICO')

df.to_csv("sample_data.csv")