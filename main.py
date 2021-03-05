# # /**************************
# # * A file to clean data for analysis in order to 
# # * replicate the analysis in the shared prosperity report. 
# # *
# *
# * Owen Kay
# * Edited: 2/5/21
# **************************/
# clear all
# capture log close
import pandas as pd
import numpy as np
import os



# * Change to file path to reflect location of your directory
# global path "/Volumes/lsa-econ-rsqe/okay/projects/detroit_middle_class/SEMCOG"
path = '/Users/tianxie/Documents/Projects/forecastModel/shared-prosperity'

# global raw_data "$path/raw_data"
raw_data = os.path.join(path, 'raw_data')
# global int_data "$path/intermediate_data"
int_data = os.path.join(path, 'intermediate_data')
# global final_data "$path/final_data"
final_data = os.path.join(path, 'final_data')
# cd $path

# /***************
# * Read in data *
# ***************/

# /*********************
# * Read and Clean PCE 
# *********************/
# import delimited "$raw_data/PCE_annual.csv", clear
pce_annual_table = pd.read_csv(os.path.join(raw_data, 'PCE_annual.csv'))
# gen year = substr(date,-4,4)
pce_annual_table.DATE = pce_annual_table.DATE.apply(lambda x: x[-4:])
# destring year, replace 
pce_annual_table.DATE = pce_annual_table.DATE.astype('int32')
pce_annual_table = pce_annual_table.rename(columns={"DATE": "year", "PCE": "pce"})
# keep year pce
# save "$int_data/pce.dta", replace
pce_annual_table.to_stata(os.path.join(int_data, 'pce.dta'), write_index=False)

# /****************************
# * Read in MSA PUMA crosswalk
# ****************************/
# import excel "$raw_data/MSA2013_PUMA2010_crosswalk.xls", sheet("MSA2013_PUMA2010_crosswalk") firstrow clear
msa_puma_crosswalk = pd.read_excel(os.path.join(raw_data, 'MSA2013_PUMA2010_crosswalk.xls'), 'MSA2013_PUMA2010_crosswalk')
# destring (MSACode StateFIPSCode PUMACode), replace
msa_puma_crosswalk['MSA Code'] = msa_puma_crosswalk['MSA Code'].astype('int32')
msa_puma_crosswalk['State FIPS Code'] = msa_puma_crosswalk['State FIPS Code'].astype('int32')
msa_puma_crosswalk['PUMA Code'] = msa_puma_crosswalk['PUMA Code'].astype('int32')
# rename (MSACode PUMACode StateFIPSCode) (met2013 puma statefip)
msa_puma_crosswalk = msa_puma_crosswalk.rename({
  "MSA Code": "met2013",
  "PUMA Code": "puma",
  "State FIPS Code": "statefip",
})


# * Rename MSAs to make them match
# replace MSATitle = "Albany-Lebanon, OR" if MSATitle == "Albany, OR"
# replace MSATitle = "Anniston-Oxford, AL" if MSATitle == "Anniston-Oxford-Jacksonville, AL"
# replace MSATitle = "Atlanta-Sandy Springs-Alpharetta, GA" if MSATitle == "Atlanta-Sandy Springs-Roswell, GA"
# replace MSATitle = "Austin-Round Rock-Georgetown, TX" if MSATitle == "Austin-Round Rock, TX"
# replace MSATitle = "Bend, OR" if MSATitle == "Bend-Redmond, OR"
# replace MSATitle = "Blacksburg-Christiansburg, VA" if MSATitle == "Blacksburg-Christiansburg-Radford, VA"
# replace MSATitle = "Bremerton-Silverdale-Port Orchard, WA" if MSATitle == "Bremerton-Silverdale, WA"
# replace MSATitle = "Buffalo-Cheektowaga, NY" if MSATitle == "Buffalo-Cheektowaga-Niagara Falls, NY"
# replace MSATitle = "Dayton-Kettering, OH" if MSATitle == "Dayton, OH"
# replace MSATitle = "Eugene-Springfield, OR" if MSATitle == "Eugene, OR"
# replace MSATitle = "Fayetteville-Springdale-Rogers, AR" if MSATitle == "Fayetteville-Springdale-Rogers, AR-MO"
# replace MSATitle = "Grand Rapids-Kentwood, MI" if MSATitle == "Grand Rapids-Wyoming, MI"
# replace MSATitle = "Greenville-Anderson, SC" if MSATitle == "Greenville-Anderson-Mauldin, SC"
# replace MSATitle = "Gulfport-Biloxi, MS" if MSATitle == "Gulfport-Biloxi-Pascagoula, MS"
# replace MSATitle = "Hartford-East Hartford-Middletown, CT" if MSATitle == "Hartford-West Hartford-East Hartford, CT"
# replace MSATitle = "Hilton Head Island-Bluffton, SC" if MSATitle == "Hilton Head Island-Bluffton-Beaufort, SC"
# replace MSATitle = "Kingsport-Bristol, TN-VA" if MSATitle == "Kingsport-Bristol-Bristol, TN-VA"
# replace MSATitle = "Macon-Bibb County, GA" if MSATitle == "Macon, GA"
# replace MSATitle = "Mankato, MN" if MSATitle == "Mankato-North Mankato, MN"
# replace MSATitle = "Miami-Fort Lauderdale-Pompano Beach, FL" if MSATitle == "Miami-Fort Lauderdale-West Palm Beach, FL"
# replace MSATitle = "Milwaukee-Waukesha, WI" if MSATitle == "Milwaukee-Waukesha-West Allis, WI"
# replace MSATitle = "Naples-Marco Island, FL" if MSATitle == "Naples-Immokalee-Marco Island, FL"
# replace MSATitle = "Niles, MI" if MSATitle == "Niles-Benton Harbor, MI"
# replace MSATitle = "Olympia-Lacey-Tumwater, WA" if MSATitle == "Olympia-Tumwater, WA"
# replace MSATitle = "Phoenix-Mesa-Chandler, AZ" if MSATitle == "Phoenix-Mesa-Scottsdale, AZ"
# replace MSATitle = "Prescott Valley-Prescott, AZ" if MSATitle == "Prescott, AZ"
# replace MSATitle = "Raleigh-Cary, NC" if MSATitle == "Raleigh, NC"
# replace MSATitle = "Sacramento-Roseville-Folsom, CA" if MSATitle == "Sacramento--Roseville--Arden-Arcade, CA"
# replace MSATitle = "San Diego-Chula Vista-Carlsbad, CA" if MSATitle == "San Diego-Carlsbad, CA"
# replace MSATitle = "San Francisco-Oakland-Berkeley, CA" if MSATitle == "San Francisco-Oakland-Hayward, CA"
# replace MSATitle = "San Luis Obispo-Paso Robles, CA" if MSATitle == "San Luis Obispo-Paso Robles-Arroyo Grande, CA"
# replace MSATitle = "Santa Rosa-Petaluma, CA" if MSATitle == "Santa Rosa, CA"
# replace MSATitle = "Scranton--Wilkes-Barre, PA" if MSATitle == "Scranton--Wilkes-Barre--Hazleton, PA"
# replace MSATitle = "Visalia, CA" if MSATitle == "Visalia-Porterville, CA"
# replace MSATitle = "Sebring-Avon Park, FL" if MSATitle == "Sebring, FL"
# replace MSATitle = "Staunton, VA" if MSATitle == "Staunton-Waynesboro, VA"
# replace MSATitle = "Stockton, CA" if MSATitle == "Stockton-Lodi, CA"
# replace MSATitle = "Trenton-Princeton, NJ" if MSATitle == "Trenton, NJ"
# replace MSATitle = "Vallejo, CA" if MSATitle == "Vallejo-Fairfield, CA"
# replace MSATitle = "Wausau-Weston, WI" if MSATitle == "Wausau, WI"


# * Keep only MSA level identifiers 
# keep met2013 MSATitle  
# duplicates drop

# save "$int_data/puma_msa_crosswalk.dta", replace
msa_puma_crosswalk.to_stata(os.path.join(int_data, 'puma_msa_crosswalk.dta'), write_index=False)


# /**************************************
# * Read in BEA RPP Metro/Nonmetro data
# ***************************************/
# import delimited "$raw_data/PARPP_PORT_2008_2019.csv", clear
bea_rpp = pd.read_csv(os.path.join(raw_data, 'PARPP_PORT_2008_2019.csv'))

# * Stata doesn't allow numbers to be variable names. Fix the variable names.
# foreach v of varlist v9-v20{
# 	local var_label : variable label `v'
# 	rename `v' rpp_all_items_nonmetro`var_label'
# }

# * Keep only Aggregate RPP Index
# keep if description == "RPPs: All items"
bea_rpp = bea_rpp[bea_rpp['Description'] == "RPPs: All items"]

# * Extract statefips code from geofips
# gen statefip = substr(geofips,3,2)
bea_rpp['statefip'] = bea_rpp['GeoFIPS'].apply(lambda x: int(x[2:4]))

# * Keep only data on Michigan Nonmetropolitan Protion
# keep if strpos(geoname,"(Nonmetropolitan Portion)") > 0 
bea_rpp = bea_rpp[
  bea_rpp.GeoName.apply(
    lambda x: "(Nonmetropolitan Portion)" in x if type(x)== str else False
    )
  ]
# keep geoname statefip rpp_all_items_nonmetro*

# * Reshape to long 
# reshape long rpp_all_items_nonmetro, i(geoname) j(year)

# * Convert statefips to numeric
# destring statefip, replace

# * Save
# save "$int_data/rpp_nonmetro_long.dta", replace
bea_year_list = [int(x) for x in bea_rpp.columns.to_list() if x.isdigit()]
bea_rpp_processed = pd.DataFrame([], columns=['geoname', 'year', 'rpp_all_items_nonmetro', 'statefip'])
for geo_name in bea_rpp['GeoName'].unique():
  for year in bea_year_list:
    val = bea_rpp[bea_rpp['GeoName']==geo_name][str(year)].values[0]
    statefip = bea_rpp[bea_rpp['GeoName']==geo_name]['statefip'].values[0] 
    bea_rpp_processed = bea_rpp_processed.append({
      "geoname": geo_name,
      "year": year,
      "rpp_all_items_nonmetro": val,
      "statefip": statefip
    }, ignore_index=True)
bea_rpp_processed['year'] = bea_rpp_processed['year'].astype('int32')
bea_rpp_processed['statefip'] = bea_rpp_processed['statefip'].astype('int32')
bea_rpp_processed.to_stata(os.path.join(int_data, 'rpp_nonmetro_long.dta'), write_index=False)


# /**************************
# * Read in BEA RPP MSA Data
# **************************/
# import delimited "$raw_data/MARPP_MSA_2008_2019.csv", clear
bea_rpp_msa = pd.read_csv(os.path.join(raw_data, 'MARPP_MSA_2008_2019.csv'))

# * Stata doesn't allow numbers to be variable names. Fix the variable names.
# foreach v of varlist v9-v20{
# 	local var_label : variable label `v'
# 	rename `v' year`var_label'
# }

# * Make all year variables numeric
# destring year*, replace force

# * Rename MSA variable and change variable to only include MSA name 
# rename geoname MSATitle

# * Keep only Aggregate RPP Index
# keep if description == "RPPs: All items"
bea_rpp_msa = bea_rpp_msa[bea_rpp_msa['Description'] == "RPPs: All items"]

# * Only keep MSA Name and RPP Value Variabes
# keep MSATitle year*

# * Drop rows that aren't MSA's and remove (Metropolitan Statistical Area) from MSATilte name
# gen msa_ind = strpos(MSATitle,"(Metropolitan Statistical Area)")
# drop if msa_ind == 0
# replace MSATitle = substr(MSATitle, 1, msa_ind-2)
# drop msa_ind
metro_text = "(Metropolitan Statistical Area)"
bea_rpp_msa = bea_rpp_msa[
  bea_rpp_msa.GeoName.apply(
    lambda x: metro_text in x if type(x)== str else False
    )
  ]
bea_rpp_msa['GeoName'] = bea_rpp_msa['GeoName'].apply(
  lambda x: x.replace("(Metropolitan Statistical Area)", "").strip() 
)

# * Merge in puma/MSA crosswalk to match with ACS Data
# merge 1:m MSATitle using "$int_data/puma_msa_crosswalk.dta", nogen keep(3)

# reshape long year, i(MSATitle) j(rpp_all_items)
# rename (year rpp_all_items) (rpp_all_items year)
# save "$int_data/rpp_msa_long.dta", replace
bea_msa_year_list = [int(x) for x in bea_rpp_msa.columns.to_list() if x.isdigit()]
bea_rpp_msa_processed = pd.DataFrame([], columns=['MSATitle', 'year', 'rpp_all_items', 'met2013'], dtype='object')
for geo_name in bea_rpp_msa['GeoName'].unique():
  for year in bea_msa_year_list:
    raw_val = bea_rpp_msa[bea_rpp_msa['GeoName']==geo_name][str(year)].values[0]
    val = None
    if type(raw_val) == str:
      try: 
        val = float(raw_val)
      except:
        bea_rpp_msa_processed = bea_rpp_msa_processed[bea_rpp_msa_processed['MSATitle'] != geo_name]
        break 
    else: 
      val = np.format_float_positional(raw_val)
    met2013 = bea_rpp_msa[bea_rpp_msa['GeoName']==geo_name]['GeoFIPS'].values[0].replace('"', '')
    bea_rpp_msa_processed = bea_rpp_msa_processed.append({
      "MSATitle": geo_name,
      "year": year,
      "rpp_all_items": val,
      "met2013": met2013
    }, ignore_index=True)
bea_rpp_msa_processed['year'] = bea_rpp_msa_processed['year'].astype('int32')
bea_rpp_msa_processed['met2013'] = bea_rpp_msa_processed['met2013'].astype('int32')
bea_rpp_msa_processed['rpp_all_items'] = bea_rpp_msa_processed['rpp_all_items'].astype('float64')
bea_rpp_msa_processed.to_stata(os.path.join(int_data, 'rpp_msa_long_test.dta'), write_index=False)


# /*******************
# * Main Analysis
# *******************/

# /*************************
# * Read in IPUMS ACS data *
# *************************/
# use "$raw_data/acs_2018_2019.dta", clear
acs_ipums = pd.read_stata(os.path.join(raw_data, 'acs_2018_2019.dta'), convert_categoricals=False)

# * Generate state puma
# gen stpuma = statefip*100000 + puma
# gen semcog = (stpuma <= 2603300 & stpuma > 2602700)
#acs_ipums = acs_ipums[acs_ipums['statefip'] == 'michigan']
acs_ipums['stpuma'] = acs_ipums[['statefip', 'puma']].apply(lambda x: x.statefip*100000 + x.puma, axis=1)
acs_ipums = acs_ipums[(acs_ipums['stpuma'] <= 2603300) & (acs_ipums['stpuma'] > 2602700)]

# * Fix incomes coded as missing 
# replace hhincome = . if hhincome == 9999999
acs_ipums['hhincome'] = acs_ipums['hhincome'].apply(lambda x: np.nan if x==9999999 else x)
# replace incwage = . if incwage == 999999
acs_ipums['incwage'] = acs_ipums['incwage'].apply(lambda x: np.nan if x==999999 else x)
# replace incwage = . if incwage == 999998
acs_ipums['incwage'] = acs_ipums['incwage'].apply(lambda x: np.nan if x==999998 else x)


# * Merge in RPP data and MSA PUMA crosswalk
# merge m:1  met2013 year using "$int_data/rpp_msa_long.dta", keep(1 3) nogen
# merge with bea_rpp_msa_processed, add MSATitle and rpp_all_items to acs_ipums
acs_ipums_merged = acs_ipums.merge(bea_rpp_msa_processed, how='left', on=['met2013', 'year'])

# * Merge in RPP data for nonmetro areas 
# merge m:1 statefip year using "$int_data/rpp_nonmetro_long.dta", keep(3) nogen
acs_ipums_merged = acs_ipums_merged.merge(bea_rpp_processed, how='left', on=['statefip', 'year'])

# * Use nonmetro RPP if not in a metro area
# replace rpp_all_items = rpp_all_items_nonmetro if rpp_all_items == .
acs_ipums_merged['rpp_all_items'] = acs_ipums_merged['rpp_all_items'].fillna(value=acs_ipums_merged['rpp_all_items_nonmetro'])

# * Create adjusted Househould income 
# gen hhi3      = hhincome*sqrt(3)/sqrt(numprec)
acs_ipums_merged['hhi3'] = acs_ipums_merged[['hhincome', 'numprec']].apply(lambda x: x.hhincome*np.sqrt(3)/np.sqrt(x.numprec), axis=1)
# gen hhi3cl    = hhi3/(rpp_all_items*.01)
acs_ipums_merged['hhi3cl'] = acs_ipums_merged[['hhi3', 'rpp_all_items']].apply(lambda x: x.hhi3/(x.rpp_all_items*0.01), axis=1)

# * Calculate Median Incomes by year and save in a temp file
# preserve 
# keep if pernum == 1
acs_ipums_merged = acs_ipums_merged[acs_ipums_merged['pernum'] == 1]
# collapse (p50) hhi3 [pw = hhwt], by(year)
weighted_hh_df = acs_ipums_merged[['hhi3', 'hhwt', 'year']]
weighted_hh_df['hhwt'] = weighted_hh_df['hhwt'].apply(lambda x: [1 for i in range(x)])
weighted_hh_df = weighted_hh_df.explode('hhwt')

hhi3_pivot_by_year = weighted_hh_df.pivot_table(values='hhi3', index=["year"], aggfunc=np.median)
hhi3_pivot_by_year['hhi3'] = hhi3_pivot_by_year['hhi3'].astype('int32')

# rename hhi3 median_hhi3
hhi3_pivot_by_year['median_hhi3'] = hhi3_pivot_by_year['hhi3'] 
del hhi3_pivot_by_year['hhi3'] 

# * Save median income in a temporary file to be merged back into ACS data
# tempfile med_inc
# save `med_inc'

# * Restore the ACS Microdata and merge in the median HH income
# restore 
# merge m:1 year using `med_inc', nogen keep(3)
acs_ipums_merged = acs_ipums_merged.merge(hhi3_pivot_by_year, how='left', on=['year'])

# * Assign classifcation based on relation between hh income to median 
# gen lower_hhi  = (hhi3cl < (2/3)*median_hhi3)
acs_ipums_merged['lower_hhi'] = acs_ipums_merged[['hhi3cl', 'median_hhi3']].apply(lambda x: x.hhi3cl < (2/3)*x.median_hhi3, axis=1)
# gen middle_hhi = (hhi3cl >= (2/3)*median_hhi3 & hhi3cl < 2*median_hhi3)
acs_ipums_merged['middle_hhi'] = acs_ipums_merged[['hhi3cl', 'median_hhi3']].apply(lambda x: (x.hhi3cl >= (2/3)*x.median_hhi3) & (x.hhi3cl < 2*x.median_hhi3), axis=1)
# gen upper_hhi  = (hhi3cl > 2*median_hhi3)
acs_ipums_merged['upper_hhi'] = acs_ipums_merged[['hhi3cl', 'median_hhi3']].apply(lambda x: x.hhi3cl > 2*x.median_hhi3,  axis=1)

# * Merge in PCE data and inflation adjust Household income
# merge m:1 year using "$int_data/pce.dta", nogen keep(3)
acs_ipums_merged = acs_ipums_merged.merge(pce_annual_table, how='left', on=['year'])

# * Store 2018 pce value in a local variable
# summ pce if year == 2018
# local pce_18 = r(mean)
pce_18 = pce_annual_table[pce_annual_table.year == 2018].pce.values[0]

# * Inflation adjust income to 2018 values
# gen hhi3cl18 = hhi3cl * (`pce_18'/pce)
acs_ipums_merged['hhi3cl18'] = acs_ipums_merged[['hhi3cl', 'pce']].apply(lambda x: x.hhi3cl * (pce_18 / x.pce), axis=1)

# * Create Race/Ethnicity variable
# gen race_hispan = "hispanic" if inlist(hispan,1,2,3,4)
acs_ipums_merged['race_hispan'] = acs_ipums_merged['hispan'].apply(lambda x: 'hispanic' if x in [1,2,3,4] else '')
# replace race_hispan = "black" if race == 2 & race_hispan == ""
acs_ipums_merged['race_hispan'] = acs_ipums_merged[['race', 'race_hispan']].apply(lambda x: 'black' if (x.race==2) & (x.race_hispan=='')  else x.race_hispan, axis=1)
# replace race_hispan = "white" if race == 1 & race_hispan == ""
acs_ipums_merged['race_hispan'] = acs_ipums_merged[['race', 'race_hispan']].apply(lambda x: 'white' if (x.race==1) & (x.race_hispan=='')  else x.race_hispan, axis=1)
# encode race_hispan, gen(race_hispan_cat)

# /*************************************
# * Get Summary Stats for SEMCOG Region
# *************************************/

# * Keep only data in SEMCOG
# keep if semcog == 1

# /************************************
# * Get mean and median income by PUMA
# ************************************/
# preserve 
# collapse (mean) hhi3cl18 (median) hhi3cl18_med = hhi3cl18 [fw=perwt], by(stpuma year)

# Apply perwt and generate income by puma summary table 
weighted_per_df = acs_ipums_merged[['hhi3cl18', 'perwt', 'year', 'stpuma']]
weighted_per_df['perwt'] = weighted_per_df['perwt'].apply(lambda x: [1 for i in range(x)])
weighted_per_df = weighted_per_df.explode('perwt')
income_by_puma = weighted_per_df.pivot_table(values='hhi3cl18', index=["stpuma","year"], aggfunc=[np.mean, np.median]).unstack()

# reshape wide hhi3cl18 hhi3cl18_med, i(stpuma) j(year)

# * Save as mean and median income by puma as csv file
# export delimited "$final_data/income_by_puma.csv", replace
income_by_puma.to_csv(os.path.join((final_data, 'income_by_puma_test.csv')))
# * Restore Microdata
# restore


# /************************************
# * Get mean and median income by PUMA
# ************************************/
# preserve 
# collapse (mean) hhi3cl18 (median) hhi3cl18_med = hhi3cl18 [fw=perwt], by(stpuma year)

# reshape wide hhi3cl18 hhi3cl18_med, i(stpuma) j(year)

# * Save as mean and median income by puma as csv file
# export delimited "$final_data/income_by_puma.csv", replace

# * Restore Microdata
# restore

# /************************************
# * Get % of income thresholds for SEMCOG by race/ethnicity
# ************************************/
# preserve 
# collapse (mean) lower_hhi middle_hhi upper_hhi [fw=perwt], by(year)
# Apply perwt and generate income by puma summary table 
weighted_total_cat_df = acs_ipums_merged[['lower_hhi', 'middle_hhi', 'upper_hhi', 'perwt', 'year']]
weighted_total_cat_df['perwt'] = weighted_total_cat_df['perwt'].apply(lambda x: [1 for i in range(x)])
weighted_total_cat_df = weighted_total_cat_df.explode('perwt')
total_cat = weighted_total_cat_df.pivot_table(values=['lower_hhi', 'middle_hhi', 'upper_hhi'], index=["year"], aggfunc=[np.mean])
total_cat.columns = total_cat.columns.droplevel(level=0)
total_cat = total_cat.rename(columns=lambda x: x+'_mean')
total_cat = total_cat.reset_index()
total_cat['race_hispan'] = pd.Series(["All race/ethnicity" for i in range(len(total_cat.index))], index=total_cat.index)

# gen race_hispan = "All race/ethnicity"

# * Save as temp file
# tempfile total_cat
# save `total_cat'

# * Restore Microdata
# restore

# * Get data by Race/Ethnicity
# collapse (mean) lower_hhi middle_hhi upper_hhi [fw=perwt], by(year race_hispan)
# append using `total_cat'
# drop if race_hispan == ""
# sort year race_hispan
# Apply perwt and generate income by puma summary table 
weighted_per_by_race_df = acs_ipums_merged[['lower_hhi', 'middle_hhi', 'upper_hhi', 'race_hispan', 'perwt', 'year']]
weighted_per_by_race_df['perwt'] = weighted_per_by_race_df['perwt'].apply(lambda x: [1 for i in range(x)])
weighted_per_by_race_df = weighted_per_by_race_df.explode('perwt')
income_by_puma = weighted_per_by_race_df.pivot_table(values=['lower_hhi', 'middle_hhi', 'upper_hhi'], index=["year", "race_hispan"], aggfunc=[np.mean])
income_by_puma.columns = income_by_puma.columns.droplevel(level=0)
income_by_puma = income_by_puma.rename(columns=lambda x: x+'_mean')
income_by_puma = income_by_puma.reset_index()
income_by_puma = income_by_puma.append(total_cat, ignore_index=True, sort=False)
income_by_puma = income_by_puma[income_by_puma['race_hispan']!=''].sort_values(by=['year', 'race_hispan']).reset_index()
del income_by_puma['index']

# * Save data 
# export delimited "$final_data/income_categories.csv", replace
income_by_puma.to_csv(os.path.join((final_data, 'income_categories_test.csv')))