/**************************
* A file to clean data for analysis in order to 
* replicate the analysis in the shared prosperity report. 
*
*
* Owen Kay
* Edited: 2/5/21
**************************/


clear all
capture log close

* Change to file path to reflect location of your directory
global path "/Volumes/lsa-econ-rsqe/okay/projects/detroit_middle_class/SEMCOG"

global raw_data "$path/raw_data"
global int_data "$path/intermediate_data"
global final_data "$path/final_data"
cd $path

/***************
* Read in data *
***************/

/*********************
* Read and Clean PCE 
*********************/
import delimited "$raw_data/PCE_annual.csv", clear
gen year = substr(date,-4,4)
destring year, replace 
keep year pce
save "$int_data/pce.dta", replace


/****************************
* Read in MSA PUMA crosswalk
****************************/
import excel "$raw_data/MSA2013_PUMA2010_crosswalk.xls", sheet("MSA2013_PUMA2010_crosswalk") firstrow clear
destring (MSACode StateFIPSCode PUMACode), replace
rename (MSACode PUMACode StateFIPSCode) (met2013 puma statefip)


* Rename MSAs to make them match
replace MSATitle = "Albany-Lebanon, OR" if MSATitle == "Albany, OR"
replace MSATitle = "Anniston-Oxford, AL" if MSATitle == "Anniston-Oxford-Jacksonville, AL"
replace MSATitle = "Atlanta-Sandy Springs-Alpharetta, GA" if MSATitle == "Atlanta-Sandy Springs-Roswell, GA"
replace MSATitle = "Austin-Round Rock-Georgetown, TX" if MSATitle == "Austin-Round Rock, TX"
replace MSATitle = "Bend, OR" if MSATitle == "Bend-Redmond, OR"
replace MSATitle = "Blacksburg-Christiansburg, VA" if MSATitle == "Blacksburg-Christiansburg-Radford, VA"
replace MSATitle = "Bremerton-Silverdale-Port Orchard, WA" if MSATitle == "Bremerton-Silverdale, WA"
replace MSATitle = "Buffalo-Cheektowaga, NY" if MSATitle == "Buffalo-Cheektowaga-Niagara Falls, NY"
replace MSATitle = "Dayton-Kettering, OH" if MSATitle == "Dayton, OH"
replace MSATitle = "Eugene-Springfield, OR" if MSATitle == "Eugene, OR"
replace MSATitle = "Fayetteville-Springdale-Rogers, AR" if MSATitle == "Fayetteville-Springdale-Rogers, AR-MO"
replace MSATitle = "Grand Rapids-Kentwood, MI" if MSATitle == "Grand Rapids-Wyoming, MI"
replace MSATitle = "Greenville-Anderson, SC" if MSATitle == "Greenville-Anderson-Mauldin, SC"
replace MSATitle = "Gulfport-Biloxi, MS" if MSATitle == "Gulfport-Biloxi-Pascagoula, MS"
replace MSATitle = "Hartford-East Hartford-Middletown, CT" if MSATitle == "Hartford-West Hartford-East Hartford, CT"
replace MSATitle = "Hilton Head Island-Bluffton, SC" if MSATitle == "Hilton Head Island-Bluffton-Beaufort, SC"
replace MSATitle = "Kingsport-Bristol, TN-VA" if MSATitle == "Kingsport-Bristol-Bristol, TN-VA"
replace MSATitle = "Macon-Bibb County, GA" if MSATitle == "Macon, GA"
replace MSATitle = "Mankato, MN" if MSATitle == "Mankato-North Mankato, MN"
replace MSATitle = "Miami-Fort Lauderdale-Pompano Beach, FL" if MSATitle == "Miami-Fort Lauderdale-West Palm Beach, FL"
replace MSATitle = "Milwaukee-Waukesha, WI" if MSATitle == "Milwaukee-Waukesha-West Allis, WI"
replace MSATitle = "Naples-Marco Island, FL" if MSATitle == "Naples-Immokalee-Marco Island, FL"
replace MSATitle = "Niles, MI" if MSATitle == "Niles-Benton Harbor, MI"
replace MSATitle = "Olympia-Lacey-Tumwater, WA" if MSATitle == "Olympia-Tumwater, WA"
replace MSATitle = "Phoenix-Mesa-Chandler, AZ" if MSATitle == "Phoenix-Mesa-Scottsdale, AZ"
replace MSATitle = "Prescott Valley-Prescott, AZ" if MSATitle == "Prescott, AZ"
replace MSATitle = "Raleigh-Cary, NC" if MSATitle == "Raleigh, NC"
replace MSATitle = "Sacramento-Roseville-Folsom, CA" if MSATitle == "Sacramento--Roseville--Arden-Arcade, CA"
replace MSATitle = "San Diego-Chula Vista-Carlsbad, CA" if MSATitle == "San Diego-Carlsbad, CA"
replace MSATitle = "San Francisco-Oakland-Berkeley, CA" if MSATitle == "San Francisco-Oakland-Hayward, CA"
replace MSATitle = "San Luis Obispo-Paso Robles, CA" if MSATitle == "San Luis Obispo-Paso Robles-Arroyo Grande, CA"
replace MSATitle = "Santa Rosa-Petaluma, CA" if MSATitle == "Santa Rosa, CA"
replace MSATitle = "Scranton--Wilkes-Barre, PA" if MSATitle == "Scranton--Wilkes-Barre--Hazleton, PA"
replace MSATitle = "Visalia, CA" if MSATitle == "Visalia-Porterville, CA"
replace MSATitle = "Sebring-Avon Park, FL" if MSATitle == "Sebring, FL"
replace MSATitle = "Staunton, VA" if MSATitle == "Staunton-Waynesboro, VA"
replace MSATitle = "Stockton, CA" if MSATitle == "Stockton-Lodi, CA"
replace MSATitle = "Trenton-Princeton, NJ" if MSATitle == "Trenton, NJ"
replace MSATitle = "Vallejo, CA" if MSATitle == "Vallejo-Fairfield, CA"
replace MSATitle = "Wausau-Weston, WI" if MSATitle == "Wausau, WI"


* Keep only MSA level identifiers 
keep met2013 MSATitle  
duplicates drop

save "$int_data/puma_msa_crosswalk.dta", replace


/**************************************
* Read in BEA RPP Metro/Nonmetro data
***************************************/
import delimited "$raw_data/PARPP_PORT_2008_2019.csv", clear

* Stata doesn't allow numbers to be variable names. Fix the variable names.
foreach v of varlist v9-v20{
	local var_label : variable label `v'
	rename `v' rpp_all_items_nonmetro`var_label'
}

* Keep only Aggregate RPP Index
keep if description == "RPPs: All items"

* Extract statefips code from geofips
gen statefip = substr(geofips,3,2)

* Keep only data on Michigan Nonmetropolitan Protion
keep if strpos(geoname,"(Nonmetropolitan Portion)") > 0 
keep geoname statefip rpp_all_items_nonmetro*

* Reshape to long 
reshape long rpp_all_items_nonmetro, i(geoname) j(year)

* Convert statefips to numeric
destring statefip, replace

* Save
save "$int_data/rpp_nonmetro_long.dta", replace

/**************************
* Read in BEA RPP MSA Data
**************************/
import delimited "$raw_data/MARPP_MSA_2008_2019.csv", clear

* Stata doesn't allow numbers to be variable names. Fix the variable names.
foreach v of varlist v9-v20{
	local var_label : variable label `v'
	rename `v' year`var_label'
}

* Make all year variables numeric
destring year*, replace force

* Rename MSA variable and change variable to only include MSA name 
rename geoname MSATitle

* Keep only Aggregate RPP Index
keep if description == "RPPs: All items"

* Only keep MSA Name and RPP Value Variabes
keep MSATitle year*

* Drop rows that aren't MSA's and remove (Metropolitan Statistical Area) from MSATilte name
gen msa_ind = strpos(MSATitle,"(Metropolitan Statistical Area)")
drop if msa_ind == 0
replace MSATitle = substr(MSATitle, 1, msa_ind-2)
drop msa_ind

* Merge in puma/MSA crosswalk to match with ACS Data
merge 1:m MSATitle using "$int_data/puma_msa_crosswalk.dta", nogen keep(3)

reshape long year, i(MSATitle) j(rpp_all_items)
rename (year rpp_all_items) (rpp_all_items year)

save "$int_data/rpp_msa_long.dta", replace


/*******************
* Main Analysis
*******************/

/*************************
* Read in IPUMS ACS data *
*************************/
use "$raw_data/acs_2018_2019.dta", clear

* Generate state puma
gen stpuma = statefip*100000 + puma
gen semcog = (stpuma <= 2603300 & stpuma > 2602700)

* Fix incomes coded as missing
replace hhincome = . if hhincome == 9999999
replace incwage = . if incwage == 999999
replace incwage = . if incwage == 999998


* Merge in RPP data and MSA PUMA crosswalk
merge m:1  met2013 year using "$int_data/rpp_msa_long.dta", keep(1 3) nogen

* Merge in RPP data for nonmetro areas 
merge m:1 statefip year using "$int_data/rpp_nonmetro_long.dta", keep(3) nogen

* Use nonmetro RPP if not in a metro area
replace rpp_all_items = rpp_all_items_nonmetro if rpp_all_items == .

* Create adjusted Househould income 
gen hhi3      = hhincome*sqrt(3)/sqrt(numprec)
gen hhi3cl    = hhi3/(rpp_all_items*.01)

* Calculate Median Incomes by year and save in a temp file
preserve 
keep if pernum == 1
collapse (p50) hhi3 [pw = hhwt], by(year)
rename hhi3 median_hhi3

* Save median income in a temporary file to be merged back into ACS data
tempfile med_inc
save `med_inc'

* Restore the ACS Microdata and merge in the median HH income
restore 
merge m:1 year using `med_inc', nogen keep(3)

* Assign classifcation based on relation between hh income to median 
gen lower_hhi  = (hhi3cl < (2/3)*median_hhi3)
gen middle_hhi = (hhi3cl >= (2/3)*median_hhi3 & hhi3cl < 2*median_hhi3)
gen upper_hhi  = (hhi3cl > 2*median_hhi3)

* Merge in PCE data and inflation adjust Household income
merge m:1 year using "$int_data/pce.dta", nogen keep(3)

* Store 2018 pce value in a local variable
summ pce if year == 2018
local pce_18 = r(mean)

* Inflation adjust income to 2018 values
gen hhi3cl18 = hhi3cl * (`pce_18'/pce)

* Create Race/Ethnicity variable
gen race_hispan = "hispanic" if inlist(hispan,1,2,3,4)
replace race_hispan = "black" if race == 2 & race_hispan == ""
replace race_hispan = "white" if race == 1 & race_hispan == ""
encode race_hispan, gen(race_hispan_cat)

/*************************************
* Get Summary Stats for SEMCOG Region
*************************************/

* Keep only data in SEMCOG
keep if semcog == 1

/************************************
* Get mean and median income by PUMA
************************************/
preserve 
collapse (mean) hhi3cl18 (median) hhi3cl18_med = hhi3cl18 [fw=perwt], by(stpuma year)

reshape wide hhi3cl18 hhi3cl18_med, i(stpuma) j(year)

* Save as mean and median income by puma as csv file
export delimited "$final_data/income_by_puma.csv", replace

* Restore Microdata
restore


/************************************
* Get mean and median income by PUMA
************************************/
preserve 
collapse (mean) hhi3cl18 (median) hhi3cl18_med = hhi3cl18 [fw=perwt], by(stpuma year)

reshape wide hhi3cl18 hhi3cl18_med, i(stpuma) j(year)

* Save as mean and median income by puma as csv file
export delimited "$final_data/income_by_puma.csv", replace

* Restore Microdata
restore

/************************************
* Get % of income thresholds for SEMCOG by race/ethnicity
************************************/
preserve 
collapse (mean) lower_hhi middle_hhi upper_hhi [fw=perwt], by(year)

gen race_hispan = "All race/ethnicity"

* Save as temp file
tempfile total_cat
save `total_cat'

* Restore Microdata
restore

* Get data by Race/Ethnicity
collapse (mean) lower_hhi middle_hhi upper_hhi [fw=perwt], by(year race_hispan)
append using `total_cat'
drop if race_hispan == ""
sort year race_hispan

* Save data 
export delimited "$final_data/income_categories.csv", replace
