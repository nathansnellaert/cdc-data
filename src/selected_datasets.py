# High-quality CDC datasets selected for ingestion
# Score > 75, updated within last 2 years
# 184 datasets total, ~183 tabular (SODA API), 1 blob file

SELECTED_DATASETS = {
    "hc4f-j6nb": 92,  # Provisional Death Counts for Coronavirus Disease (COVID-19)
    "xkkf-xrst": 90,  # Excess Deaths Associated with COVID-19
    "r8kw-7aab": 99,  # Provisional COVID-19 Death Counts by Week Ending Date and State
    "9bhg-hcku": 90,  # Provisional COVID-19 Deaths by Sex and Age
    "uggs-hy5q": 90,  # Provisional COVID-19 Deaths by Place of Death and State
    "bi63-dtpu": 85,  # NCHS - Leading Causes of Death: United States
    "hn4x-zwk7": 85,  # Nutrition, Physical Activity, and Obesity - Behavioral Risk Factor Surveillance System
    "vsak-wrfu": 92,  # Provisional COVID-19 Deaths by Week, Sex, and Age
    "xkb8-kh2a": 99,  # VSRR Provisional Drug Overdose Death Counts
    "9j2v-jamp": 85,  # Death rates for suicide, by sex, race, Hispanic origin, and age: United States
    "y5bj-9g5w": 90,  # Weekly Counts of Deaths by Jurisdiction and Age
    "w9j2-ggv5": 85,  # NCHS - Death rates and life expectancy at birth
    "u6jv-9ijr": 90,  # Weekly Counts of Death by Jurisdiction and Select Causes of Death
    "k8wy-p9cg": 88,  # Provisional COVID-19 Deaths by County, and Race and Hispanic Origin
    "2ew6-ywp6": 98,  # NWSS Public SARS-CoV-2 Wastewater Metric Data
    "8pt5-q6wp": 90,  # Indicators of Anxiety or Depression Based on Reported Frequency of Symptoms During Last 7 Days
    "yni7-er2q": 90,  # Mental Health Care in the Last 4 Weeks
    "pj7m-y5uh": 88,  # Provisional COVID-19 Deaths: Distribution of Deaths by Race and Hispanic Origin
    "hmz2-vwda": 92,  # VSRR - State and National Provisional Counts for Live Births, Deaths, and Infant Deaths
    "kn79-hsxy": 90,  # Provisional COVID-19 Death Counts in the United States by County
    "489q-934x": 95,  # NCHS - VSRR Quarterly provisional estimates for selected indicators of mortality
    "95ax-ymtc": 90,  # Drug overdose death rates, by drug type, sex, age, race, and Hispanic origin: United States
    "ks3g-spdg": 88,  # Provisional COVID-19 Deaths by Race and Hispanic Origin, and Age
    "v6ab-adf5": 85,  # NCHS - Childhood Mortality Rates
    "735e-byxc": 85,  # Nutrition, Physical Activity, and Obesity - Women, Infant, and Child
    "dttw-5yxu": 80,  # Behavioral Risk Factor Surveillance System (BRFSS) Prevalence Data (2011 to present)
    "g653-rqe2": 98,  # NWSS Public SARS-CoV-2 Concentration in Wastewater Data
    "nr4s-juj3": 88,  # Provisional COVID-19 Deaths: Focus on Ages 0-18 Years
    "25m4-6qqq": 85,  # NHIS Adult Summary Health Statistics
    "qfhf-uhaa": 90,  # Weekly Counts of Deaths by Jurisdiction, and Race and Hispanic Origin
    "e6fc-ccez": 90,  # NCHS - Births and General Fertility Rates: United States
    "29hc-w46k": 99,  # Weekly Rates of Laboratory-Confirmed RSV Hospitalizations from the RSV-NET Surveillance System
    "89yk-m38d": 85,  # NCHS - Natality Measures for Females by Race and Hispanic Origin: United States
    "jr58-6ysp": 99,  # SARS-CoV-2 Variant Proportions
    "pbkm-d27e": 88,  # NCHS - Drug Poisoning Mortality by County: United States
    "mpx5-t7tu": 96,  # Provisional COVID-19 death counts, rates, and percent of total deaths, by jurisdiction of residence
    "6rkc-nb2q": 85,  # NCHS - Age-adjusted Death Rates for Selected Major Causes of Death
    "3h58-x6cd": 80,  # NCHS - Teen Birth Rates for Age Group 15-19 in the United States by County
    "8mrp-rmkw": 95,  # Nutrition, Physical Activity, and Obesity - American Community Survey
    "xbxb-epbu": 90,  # NCHS - Drug Poisoning Mortality by State: United States
    "y268-sna3": 85,  # NCHS - U.S. and State Trends on Teen Births
    "2m93-xvra": 85,  # Health conditions among children under age 18, by selected characteristics: United States
    "jx6g-fdh6": 90,  # NCHS - Drug Poisoning Mortality by State: United States
    "cf5u-bm9w": 99,  # Monthly Rates of Laboratory-Confirmed COVID-19 Hospitalizations from the COVID-NET Surveillance System
    "rpvx-m2md": 90,  # NCHS - Drug Poisoning Mortality by County: United States
    "vdpk-qzpr": 85,  # NCHS - Potentially Excess Deaths from the Five Leading Causes of Death
    "vba9-s8jp": 80,  # Nutrition, Physical Activity, and Obesity - Youth Risk Behavior Surveillance System
    "3nzu-udr9": 85,  # Normal weight, overweight, and obesity among adults aged 20 and over, by selected characteristics: United States
    "d2rk-yvas": 80,  # Behavioral Risk Factor Surveillance System (BRFSS) Age-Adjusted Prevalence Data (2011 to present)
    "e8kx-wbww": 85,  # NCHS - Teen Birth Rates for Females by Age Group, Race, and Hispanic Origin: United States
    "tpcp-uiv5": 90,  # Provisional COVID-19 Deaths by HHS Region, Race, and Age
    "nt65-c7a7": 90,  # NCHS - Injury Mortality: United States
    "x9gk-5huc": 100,  # NNDSS Weekly Data
    "yt7u-eiyg": 85,  # NCHS - Birth Rates for Females by Age Group: United States
    "9gay-j69q": 90,  # Obesity among children and adolescents aged 2–19 years, by selected characteristics: United States
    "k5fw-7ju6": 95,  # Unknown dataset
    "6tkz-y37d": 85,  # NCHS - Birth Rates for Unmarried Women by Age, Race, and Hispanic Origin: United States
    "ynw2-4viq": 92,  # Provisional Death Counts for Influenza, Pneumonia, and COVID-19
    "4va6-ph5s": 90,  # Provisional COVID-19 Deaths by Place of Death and Age
    "p56q-jrxg": 88,  # NCHS - Drug Poisoning Mortality by County: United States
    "76vv-a7x8": 95,  # NCHS - VSRR Quarterly provisional estimates for selected birth indicators
    "g6qk-ngsf": 90,  # NCHS - Births to Unmarried Women by Age Group: United States
    "jqwm-z2g9": 95,  # NCHS - VSRR Quarterly provisional estimates for infant mortality
    "isx2-c2ii": 88,  # NCHS - Percent Distribution of Births for Females by Age Group: United States
    "s54h-bixi": 85,  # NCHS - Natality Measures for Females by Hispanic Origin Subgroup: United States
    "8hxn-cvik": 85,  # Nutrition, Physical Activity, and Obesity - National Immunization Survey (Breastfeeding)
    "65mz-jvh5": 90,  # AH Monthly Provisional Counts of Deaths for Select Causes of Death by Sex, Age, and Race and Hispanic Origin
    "hzd8-r9mj": 88,  # NCHS - Percent Distribution of Births to Unmarried Women by Age Group: United States
    "gb4e-yj24": 94,  # VSRR Provisional County-Level Drug Overdose Death Counts
    "jb9g-gnvr": 85,  # Indicators of Health Insurance Coverage at the Time of Interview
    "gypc-kpgn": 97,  # Drug Use Data from Selected Hospitals
    "hdy7-e2a3": 88,  # NCHS - Pregnancy Rates, by Age for Hispanic Women: United States, 1990-2010
    "m74n-4hbs": 88,  # AH Excess Deaths by Sex, Age, and Race and Hispanic Origin
    "th9n-ghnr": 80,  # Reduced Access to Care During COVID-19
    "rdmq-nq56": 98,  # NSSP Emergency Department Visit Trajectories by State and Sub State Regions- COVID-19, Flu, RSV, Combined
    "6jg4-xsqq": 99,  # Weekly Rates of Laboratory-Confirmed COVID-19 Hospitalizations from the COVID-NET Surveillance System
    "hj2x-85ya": 80,  # CDC STATE System Tobacco Legislation - Preemption Summary
    "kwbr-syv2": 80,  # CDC STATE System E-Cigarette Legislation - Tax
    "2snk-eav4": 80,  # CDC STATE System Tobacco Legislation - Smokefree Indoor Air Summary
    "mqmc-4b9n": 85,  # AH Provisional COVID-19 Deaths by Hospital Referral Region
    "44rk-q6r2": 90,  # NCHS - Drug Poisoning Mortality by State: United States
    "e2d5-ggg7": 96,  # VSRR Provisional Maternal Death Counts and Rates
    "8zea-kwnt": 80,  # CDC STATE System E-Cigarette Legislation - Youth Access
    "jwta-jxbg": 88,  # Distribution of COVID-19 Deaths and Populations, by Jurisdiction, Age, and Race and Hispanic Origin
    "xb3p-q62w": 88,  # Indicators of Reduced Access to Care Due to the Coronavirus Pandemic During Last 4 Weeks
    "kvib-3txy": 99,  # Rates of Laboratory-Confirmed RSV, COVID-19, and Flu Hospitalizations from the RESP-NET Surveillance Systems
    "32fd-hyzc": 80,  # CDC STATE System Tobacco Legislation - Smokefree Indoor Air
    "2dwv-vfam": 80,  # CDC STATE System Tobacco Legislation - Tax
    "gvsb-yw6g": 99,  # Percent Positivity of COVID-19 Nucleic Acid Amplification Tests by HHS Region, National Respiratory and Enteric Virus Surveillance System
    "gsea-w83j": 85,  # Post-COVID Conditions
    "eb4y-d4ic": 80,  # CDC STATE System Tobacco Legislation - Licensure
    "3cxc-4k8q": 99,  # Percent Positivity of Respiratory Syncytial Virus Nucleic Acid Amplification Tests by HHS Region, National Respiratory and Enteric Virus Surveillance System
    "8xy9-ubqz": 85,  # Access and Use of Telemedicine During COVID-19
    "hgv5-3wrn": 80,  # CDC STATE System Tobacco Legislation - Youth Access
    "q3t8-zr7t": 90,  # COVID-19 Hospital Data from the National Hospital Care Survey
    "ne52-uraz": 80,  # CDC STATE System E-Cigarette Legislation - Licensure
    "aie4-agrk": 90,  # AH Monthly Provisional COVID-19 Deaths, by Census Region, Age, and Race and Hispanic Origin
    "piju-vf3p": 80,  # CDC STATE System E-Cigarette Legislation - Preemption
    "qgkx-mswu": 85,  # Loss of Work Due to Illness from COVID-19
    "tdbk-8ubw": 85,  # National Health and Nutrition Examination Survey (NHANES) – Vision and Eye Health Surveillance
    "trpk-sp8z": 90,  # NHIS Interactive Biannual Early Release Estimates
    "xsta-sbh5": 80,  # CDC STATE System Tobacco Legislation - Preemption
    "yhkp-cczf": 80,  # CDC STATE System Tobacco Legislation - Smokefree Campus
    "wan8-w4er": 80,  # CDC STATE System E-Cigarette Legislation - Smokefree Indoor Air
    "vkwg-yswv": 80,  # Behavioral Risk Factors – Vision and Eye Health Surveillance
    "cchw-gdwa": 80,  # 2022 Final Assisted Reproductive Technology (ART) Success Rates
    "itia-u6fu": 80,  # CDC STATE System E-Cigarette Legislation - Smokefree Campus
    "h7xa-837u": 88,  # Telemedicine Use in the Last 4 Weeks
    "ua7e-t2fy": 98,  # Weekly Hospital Respiratory Data (HRD) Metrics by Jurisdiction, National Healthcare Safety Network (NHSN)
    "3ts8-hsrw": 85,  # AH Provisional COVID-19 Deaths by Educational Attainment, Race, Sex, and Age
    "xt86-xqxz": 90,  # Visits to physician offices, hospital outpatient departments, and hospital emergency departments, by age, sex, and race: United States
    "8hzs-zshh": 92,  # Provisional drug overdose death counts for specific drugs
    "ycxr-emue": 90,  # Estimates of Emergency Department Visits in the United States from 2016-2022
    "9tjt-seye": 80,  # 2022 Final Assisted Reproductive Technology (ART) Summary
    "yrur-wghw": 96,  # Provisional COVID-19 death counts and rates by month, jurisdiction of residence, and demographic characteristics
    "vc9m-u7tv": 90,  # NCHS - Injury Mortality: United States
    "7xva-uux8": 98,  # NSSP Emergency Department Visits - COVID-19, Flu, RSV, Combined – by Demographic Category
    "s2qv-b27b": 85,  # DHDS - Prevalence of Disability Status and Types
    "r5pw-bk5t": 85,  # AH Monthly Provisional Counts of Deaths for Select Causes of Death by Age, and Race and Hispanic Origin
    "8bda-nhxv": 95,  # Active Bacterial Core surveillance (ABCs) Neisseria meningitidis
    "g7hk-rc8d": 90,  # AH Provisional COVID-19 Deaths by Week, Place of Death, and Age
    "bigw-pgk2": 99,  # Patient Characteristics of Laboratory-Confirmed COVID-19 Hospitalizations from the COVID-NET Surveillance System
    "i6ej-9eac": 85,  # AH Provisional COVID-19 Deaths by Race and Educational Attainment
    "2t2r-sf6s": 80,  # National Health Interview Survey (NHIS) – Vision and Eye Health Surveillance
    "jxu8-x79m": 80,  # State Tobacco Related Disparities Dashboard Data
    "seuz-s2cv": 99,  # Percent of Tests Positive for Viral Respiratory Pathogens
    "v2g4-wqg2": 92,  # Early Model-based Provisional Estimates of Drug Overdose, Suicide, and Transportation-related Deaths
    "8wmh-yzz9": 90,  # NHANES Select Mean Dietary Intake Estimates
    "nfuu-hu6j": 90,  # Infant, neonatal, postneonatal, fetal, and perinatal mortality rates, by detailed race and Hispanic origin of mother: United States
    "exs3-hbne": 95,  # Monthly COVID-19 Death Rates per 100,000 Population by Age Group, Race and Ethnicity, Sex, and Region with Double Stratification
    "e28h-tx85": 80,  # Medicare Fee for Service (FFS) claims (100%) – Vision and Eye Health Surveillance
    "ggsw-596z": 85,  # NHANES Select Oral Health Prevalence Estimates
    "5xkq-dg7x": 80,  # NORS
    "jqg8-ycmh": 90,  # AH Quarterly Excess Deaths by State, Sex, Age, and Race
    "vutn-jzwm": 98,  # 2023 Respiratory Virus Response - NSSP Emergency Department Visits - COVID-19, Flu, RSV, Combined
    "i667-sjhg": 90,  # NHANES Select Chronic Conditions Prevalence Estimates
    "hkhc-f7hg": 88,  # Provisional COVID-19 Deaths by Week and Urbanicity
    "53g5-jf7x": 98,  # Provisional Percent of Deaths for COVID-19, Influenza, and RSV by Select Characteristics
    "ix4g-rt8v": 80,  # 2022 Final Assisted Reproductive Technology (ART) Services and Profiles
    "w4cs-jspc": 90,  # Initial injury-related visits to hospital emergency departments, by sex, age, and intent and mechanism of injury: United States
    "pbq2-7wr2": 99,  # Monthly Rates of Laboratory-Confirmed RSV Hospitalizations from the RSV-NET Surveillance System
    "5c6r-xi2t": 98,  # Weekly Respiratory Virus Vaccination Data, Children 6 Months-17 Years and Adults 18 Years and Older, National Immunization Survey
    "dmzy-x2ad": 90,  # Delay or nonreceipt of needed medical care, prescription drugs, or dental care during the past 12 months due to cost: United States
    "wibz-pb5q": 95,  # Biennial Overview of Post-acute and Long-term Care in the United States: Data from the National Post-acute and Long-term Care Study
    "qvzb-qs6p": 99,  # 1998-2023 Serotype Data for Invasive Pneumococcal Disease Cases by Age Group and Site from Active Bacterial Core surveillance
    "9hdi-ekmb": 88,  # Provisional COVID-19 Deaths by Week and County Social Vulnerability Index
    "wxz7-ekz9": 85,  # NHIS Child Summary Health Statistics
    "dmnu-8erf": 90,  # Provisional COVID-19 death counts and rates, by jurisdiction of residence and demographic characteristics
    "a35h-9yn4": 80,  # Commercial Medical Insurance (MSCANCC) - Vision and Eye Health Surveillance
    "6ryw-hetw": 80,  # 2021 Final Assisted Reproductive Technology (ART) Patient and Cycle Characteristics
    "abgz-qs4g": 90,  # HAICViz - CDI
    "f6ee-eq37": 80,  # Table of Smokefree Non-Tribal Gaming Facilities (CDC STATE System Tobacco Legislation- Smokefree Indoor Air)
    "8s4r-kzwb": 80,  # Table of Preemption on Smokefree Indoor Air (CDC STATE System Tobacco Legislation - Preemption)
    "9xc7-3a4q": 85,  # AH Provisional COVID-19 Deaths by HHS Region, Race, Age 65plus
    "jbhn-e8xn": 90,  # BEAM Dashboard - Report Data
    "sw5n-wg2p": 95,  # Weekly Influenza Vaccination Coverage and Intent for Vaccination, Overall, by Selected Demographics and Jurisdiction, Among Adults 18 Years and Older
    "7873-6w4v": 85,  # AH Provisional COVID-19 Deaths Counts by Health Service Area
    "ey8b-ejrf": 80,  # 2021 Final Assisted Reproductive Technology (ART) Success Rates
    "wpti-gvdi": 90,  # NHIS Interactive Quarterly Early Release Estimates
    "km5s-4339": 90,  # Medicaid coverage among persons under age 65, by selected characteristics: United States
    "pqn7-e45s": 88,  # NHANES Select Infectious Diseases Prevalence Estimates
    "ikd3-vynf": 85,  # AH Provisional COVID-19 Deaths by Week and Age
    "4bc2-bbpq": 98,  # Provisional Percent of Deaths for COVID-19, Influenza, and RSV
    "88eg-qzed": 80,  # CDC STATE System E-Cigarette Legislation - Preemption Summary
    "wrev-kwxu": 80,  # 2022 Final Assisted Reproductive Technology (ART) Patient and Cycle Characteristics
    "j9g8-acpt": 99,  # CDC Wastewater Data for SARS-CoV-2
    "ite7-j2w7": 85,  # AH COVID-19 Death Counts by County and Week, 2020-present
    "en3s-hzsr": 95,  # Active Bacterial Core surveillance (ABCs) Streptococcus pneumoniae
    "i8t6-whzd": 80,  # CDC STATE System E-Cigarette Legislation - Smokefree Indoor Air Summary
    "ysd3-txwj": 95,  # Weekly Cumulative Estimated Number of Influenza Vaccinations Administered in Pharmacies and Physician Medical Offices, Adults 18 years and older, by Flu Season and Age Group, United States
    "3j26-kg6d": 85,  # Long-term Care and COVID-19
    "k5dc-apj8": 90,  # AH Provisional COVID-19 Deaths by HHS Region, Race, and Age, 2015 to date
    "dnhi-s2bf": 80,  # AH Provisional COVID-19 Death Counts by Quarter and County
    "9z9x-g48e": 85,  # AH Provisional COVID-19 Deaths by Week, Sex, and Race and Hispanic Origin
    "wcfv-gpn6": 80,  # Table of Medicaid Barriers to Treatments (Lung Association Cessation Coverage)
    "mpgq-jmmr": 99,  # Weekly Hospital Respiratory Data (HRD) Metrics by Jurisdiction, National Healthcare Safety Network (NHSN) (Preliminary)
    "ssz5-s49e": 90,  # HAICViz - iSA
    "kk8c-wtm4": 85,  # Physician Experiences Related to COVID-19 from the National Ambulatory Medical Care Survey
    "a5h2-p2dw": 80,  # Table of Preemption on Licensure (CDC STATE System Tobacco Legislation - Preemption)
    "95m5-agj4": 95,  # Active Bacterial Core surveillance (ABCs) Group B Streptococcus
    "f3zz-zga5": 99,  # Level of Acute Respiratory Illness (ARI) Activity by State
    "9y49-tura": 95,  # Active Bacterial Core surveillance (ABCs) Group A Streptococcus
    "ui6g-vumy": 80,  # 2021 Final Assisted Reproductive Technology (ART) Services and Profiles
    "uxwq-vny5": 95,  # Active Bacterial Core surveillance (ABCs) Haemophilus influenzae
}
