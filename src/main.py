#!/usr/bin/env python3
"""Main orchestrator for CDC Data integration."""

import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment
from ingest import datasets as ingest_datasets
from ingest import health_indicators as ingest_health_indicators
from ingest import raw_data as ingest_raw_data
from transforms.abcs_group_a_strep import main as transform_abcs_group_a_strep
from transforms.abcs_group_b_strep import main as transform_abcs_group_b_strep
from transforms.abcs_meningitis import main as transform_abcs_meningitis
from transforms.abcs_pneumococcal import main as transform_abcs_pneumococcal
from transforms.adult_obesity_trends import main as transform_adult_obesity_trends
from transforms.age_adjusted_death_rates import main as transform_age_adjusted_death_rates
from transforms.anxiety_depression import main as transform_anxiety_depression
from transforms.birth_fertility_rates import main as transform_birth_fertility_rates
from transforms.birth_indicators_quarterly import main as transform_birth_indicators_quarterly
from transforms.birth_rates_unmarried import main as transform_birth_rates_unmarried
from transforms.breastfeeding_nis import main as transform_breastfeeding_nis
from transforms.brfss_obesity import main as transform_brfss_obesity
from transforms.brfss_prevalence import main as transform_brfss_prevalence
from transforms.child_health_conditions import main as transform_child_health_conditions
from transforms.child_obesity_trends import main as transform_child_obesity_trends
from transforms.childhood_mortality import main as transform_childhood_mortality
from transforms.county_drug_overdose_deaths import main as transform_county_drug_overdose_deaths
from transforms.covid_death_rates_monthly import main as transform_covid_death_rates_monthly
from transforms.covid_deaths_age_race import main as transform_covid_deaths_age_race
from transforms.covid_deaths_county import main as transform_covid_deaths_county
from transforms.covid_deaths_county_race import main as transform_covid_deaths_county_race
from transforms.covid_deaths_demographics import main as transform_covid_deaths_demographics
from transforms.covid_deaths_hhs_region import main as transform_covid_deaths_hhs_region
from transforms.covid_deaths_hospital_region import main as transform_covid_deaths_hospital_region
from transforms.covid_deaths_jurisdiction import main as transform_covid_deaths_jurisdiction
from transforms.covid_deaths_place import main as transform_covid_deaths_place
from transforms.covid_deaths_race_distribution import main as transform_covid_deaths_race_distribution
from transforms.covid_deaths_sex_age import main as transform_covid_deaths_sex_age
from transforms.covid_deaths_state import main as transform_covid_deaths_state
from transforms.covid_deaths_youth import main as transform_covid_deaths_youth
from transforms.covid_hospitalizations import main as transform_covid_hospitalizations
from transforms.covid_hospitalizations_monthly import main as transform_covid_hospitalizations_monthly
from transforms.covid_test_positivity import main as transform_covid_test_positivity
from transforms.covid_variant_proportions import main as transform_covid_variant_proportions
from transforms.covid_variant_weekly import main as transform_covid_variant_weekly
from transforms.covidnet_hospitalizations import main as transform_covidnet_hospitalizations
from transforms.deaths_percent_respiratory import main as transform_deaths_percent_respiratory
from transforms.deaths_race_ethnicity import main as transform_deaths_race_ethnicity
from transforms.drug_overdose_deaths import main as transform_drug_overdose_deaths
from transforms.drug_overdose_rates import main as transform_drug_overdose_rates
from transforms.drug_overdose_specific import main as transform_drug_overdose_specific
from transforms.drug_poisoning_county import main as transform_drug_poisoning_county
from transforms.drug_poisoning_modeled import main as transform_drug_poisoning_modeled
from transforms.drug_poisoning_state import main as transform_drug_poisoning_state
from transforms.ecig_licensure import main as transform_ecig_licensure
from transforms.ecig_smokefree_indoor import main as transform_ecig_smokefree_indoor
from transforms.ed_visit_trends import main as transform_ed_visit_trends
from transforms.ed_visits_respiratory import main as transform_ed_visits_respiratory
from transforms.excess_deaths import main as transform_excess_deaths
from transforms.excess_deaths_causes import main as transform_excess_deaths_causes
from transforms.flu_pneumonia_covid_deaths import main as transform_flu_pneumonia_covid_deaths
from transforms.hai_cdi import main as transform_hai_cdi
from transforms.hospital_drug_use import main as transform_hospital_drug_use
from transforms.infant_mortality_quarterly import main as transform_infant_mortality_quarterly
from transforms.leading_causes_death import main as transform_leading_causes_death
from transforms.life_expectancy import main as transform_life_expectancy
from transforms.maternal_deaths import main as transform_maternal_deaths
from transforms.mental_health_care import main as transform_mental_health_care
from transforms.monthly_deaths_causes import main as transform_monthly_deaths_causes
from transforms.natality_measures import main as transform_natality_measures
from transforms.nchs_drug_poisoning_state import main as transform_nchs_drug_poisoning_state
from transforms.nhanes_dietary import main as transform_nhanes_dietary
from transforms.nhis_adult_health import main as transform_nhis_adult_health
from transforms.nhis_vision import main as transform_nhis_vision
from transforms.nndss_weekly import main as transform_nndss_weekly
from transforms.outbreak_nors import main as transform_outbreak_nors
from transforms.physical_activity_acs import main as transform_physical_activity_acs
from transforms.provisional_drug_overdose import main as transform_provisional_drug_overdose
from transforms.quarterly_death_rates import main as transform_quarterly_death_rates
from transforms.respiratory_hospitalizations_combined import main as transform_respiratory_hospitalizations_combined
from transforms.respiratory_vaccination import main as transform_respiratory_vaccination
from transforms.rsv_hospitalizations import main as transform_rsv_hospitalizations
from transforms.rsv_hospitalizations_weekly import main as transform_rsv_hospitalizations_weekly
from transforms.rsv_test_positivity import main as transform_rsv_test_positivity
from transforms.suicide_death_rates import main as transform_suicide_death_rates
from transforms.teen_births_county import main as transform_teen_births_county
from transforms.teen_births_race import main as transform_teen_births_race
from transforms.teen_births_trends import main as transform_teen_births_trends
from transforms.telemedicine_covid import main as transform_telemedicine_covid
from transforms.tobacco_legislation_tax import main as transform_tobacco_legislation_tax
from transforms.tobacco_preemption import main as transform_tobacco_preemption
from transforms.tobacco_smokefree_indoor import main as transform_tobacco_smokefree_indoor
from transforms.vital_statistics_monthly import main as transform_vital_statistics_monthly
from transforms.wastewater_covid_concentration import main as transform_wastewater_covid_concentration
from transforms.wastewater_covid_metrics import main as transform_wastewater_covid_metrics
from transforms.wastewater_public import main as transform_wastewater_public
from transforms.weekly_deaths_age import main as transform_weekly_deaths_age
from transforms.weekly_deaths_cause import main as transform_weekly_deaths_cause
from transforms.wic_obesity import main as transform_wic_obesity
from transforms.youth_access_legislation import main as transform_youth_access_legislation
from transforms.youth_nutrition_obesity import main as transform_youth_nutrition_obesity
from transforms.yrbs_obesity import main as transform_yrbs_obesity


def main():
    parser = argparse.ArgumentParser(description="CDC Data Connector")
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")

        print("\n--- Datasets ---")
        ingest_datasets.run()

        print("\n--- Health Indicators ---")
        ingest_health_indicators.run()

        print("\n--- Raw Data (Selected Datasets) ---")
        ingest_raw_data.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")

        print("\n--- ABCs Group A Strep ---")
        transform_abcs_group_a_strep.run()

        print("\n--- ABCs Group B Strep ---")
        transform_abcs_group_b_strep.run()

        print("\n--- ABCs Meningitis ---")
        transform_abcs_meningitis.run()

        print("\n--- ABCs Pneumococcal ---")
        transform_abcs_pneumococcal.run()

        print("\n--- Adult Obesity Trends ---")
        transform_adult_obesity_trends.run()

        print("\n--- Age Adjusted Death Rates ---")
        transform_age_adjusted_death_rates.run()

        print("\n--- Anxiety Depression ---")
        transform_anxiety_depression.run()

        print("\n--- Birth Fertility Rates ---")
        transform_birth_fertility_rates.run()

        print("\n--- Birth Indicators Quarterly ---")
        transform_birth_indicators_quarterly.run()

        print("\n--- Birth Rates Unmarried ---")
        transform_birth_rates_unmarried.run()

        print("\n--- Breastfeeding NIS ---")
        transform_breastfeeding_nis.run()

        print("\n--- BRFSS Obesity ---")
        transform_brfss_obesity.run()

        print("\n--- BRFSS Prevalence ---")
        transform_brfss_prevalence.run()

        print("\n--- Child Health Conditions ---")
        transform_child_health_conditions.run()

        print("\n--- Child Obesity Trends ---")
        transform_child_obesity_trends.run()

        print("\n--- Childhood Mortality ---")
        transform_childhood_mortality.run()

        print("\n--- County Drug Overdose Deaths ---")
        transform_county_drug_overdose_deaths.run()

        print("\n--- COVID Death Rates Monthly ---")
        transform_covid_death_rates_monthly.run()

        print("\n--- COVID Deaths Age Race ---")
        transform_covid_deaths_age_race.run()

        print("\n--- COVID Deaths County ---")
        transform_covid_deaths_county.run()

        print("\n--- COVID Deaths County Race ---")
        transform_covid_deaths_county_race.run()

        print("\n--- COVID Deaths Demographics ---")
        transform_covid_deaths_demographics.run()

        print("\n--- COVID Deaths HHS Region ---")
        transform_covid_deaths_hhs_region.run()

        print("\n--- COVID Deaths Hospital Region ---")
        transform_covid_deaths_hospital_region.run()

        print("\n--- COVID Deaths Jurisdiction ---")
        transform_covid_deaths_jurisdiction.run()

        print("\n--- COVID Deaths Place ---")
        transform_covid_deaths_place.run()

        print("\n--- COVID Deaths Race Distribution ---")
        transform_covid_deaths_race_distribution.run()

        print("\n--- COVID Deaths Sex Age ---")
        transform_covid_deaths_sex_age.run()

        print("\n--- COVID Deaths State ---")
        transform_covid_deaths_state.run()

        print("\n--- COVID Deaths Youth ---")
        transform_covid_deaths_youth.run()

        print("\n--- COVID Hospitalizations ---")
        transform_covid_hospitalizations.run()

        print("\n--- COVID Hospitalizations Monthly ---")
        transform_covid_hospitalizations_monthly.run()

        print("\n--- COVID Test Positivity ---")
        transform_covid_test_positivity.run()

        print("\n--- COVID Variant Proportions ---")
        transform_covid_variant_proportions.run()

        print("\n--- COVID Variant Weekly ---")
        transform_covid_variant_weekly.run()

        print("\n--- COVID-NET Hospitalizations ---")
        transform_covidnet_hospitalizations.run()

        print("\n--- Deaths Percent Respiratory ---")
        transform_deaths_percent_respiratory.run()

        print("\n--- Deaths Race Ethnicity ---")
        transform_deaths_race_ethnicity.run()

        print("\n--- Drug Overdose Deaths ---")
        transform_drug_overdose_deaths.run()

        print("\n--- Drug Overdose Rates ---")
        transform_drug_overdose_rates.run()

        print("\n--- Drug Overdose Specific ---")
        transform_drug_overdose_specific.run()

        print("\n--- Drug Poisoning County ---")
        transform_drug_poisoning_county.run()

        print("\n--- Drug Poisoning Modeled ---")
        transform_drug_poisoning_modeled.run()

        print("\n--- Drug Poisoning State ---")
        transform_drug_poisoning_state.run()

        print("\n--- E-Cig Licensure ---")
        transform_ecig_licensure.run()

        print("\n--- E-Cig Smokefree Indoor ---")
        transform_ecig_smokefree_indoor.run()

        print("\n--- ED Visit Trends ---")
        transform_ed_visit_trends.run()

        print("\n--- ED Visits Respiratory ---")
        transform_ed_visits_respiratory.run()

        print("\n--- Excess Deaths ---")
        transform_excess_deaths.run()

        print("\n--- Excess Deaths Causes ---")
        transform_excess_deaths_causes.run()

        print("\n--- Flu Pneumonia COVID Deaths ---")
        transform_flu_pneumonia_covid_deaths.run()

        print("\n--- HAI CDI ---")
        transform_hai_cdi.run()

        print("\n--- Hospital Drug Use ---")
        transform_hospital_drug_use.run()

        print("\n--- Infant Mortality Quarterly ---")
        transform_infant_mortality_quarterly.run()

        print("\n--- Leading Causes Death ---")
        transform_leading_causes_death.run()

        print("\n--- Life Expectancy ---")
        transform_life_expectancy.run()

        print("\n--- Maternal Deaths ---")
        transform_maternal_deaths.run()

        print("\n--- Mental Health Care ---")
        transform_mental_health_care.run()

        print("\n--- Monthly Deaths Causes ---")
        transform_monthly_deaths_causes.run()

        print("\n--- Natality Measures ---")
        transform_natality_measures.run()

        print("\n--- NCHS Drug Poisoning State ---")
        transform_nchs_drug_poisoning_state.run()

        print("\n--- NHANES Dietary ---")
        transform_nhanes_dietary.run()

        print("\n--- NHIS Adult Health ---")
        transform_nhis_adult_health.run()

        print("\n--- NHIS Vision ---")
        transform_nhis_vision.run()

        print("\n--- NNDSS Weekly ---")
        transform_nndss_weekly.run()

        print("\n--- Outbreak NORS ---")
        transform_outbreak_nors.run()

        print("\n--- Physical Activity ACS ---")
        transform_physical_activity_acs.run()

        print("\n--- Provisional Drug Overdose ---")
        transform_provisional_drug_overdose.run()

        print("\n--- Quarterly Death Rates ---")
        transform_quarterly_death_rates.run()

        print("\n--- Respiratory Hospitalizations Combined ---")
        transform_respiratory_hospitalizations_combined.run()

        print("\n--- Respiratory Vaccination ---")
        transform_respiratory_vaccination.run()

        print("\n--- RSV Hospitalizations ---")
        transform_rsv_hospitalizations.run()

        print("\n--- RSV Hospitalizations Weekly ---")
        transform_rsv_hospitalizations_weekly.run()

        print("\n--- RSV Test Positivity ---")
        transform_rsv_test_positivity.run()

        print("\n--- Suicide Death Rates ---")
        transform_suicide_death_rates.run()

        print("\n--- Teen Births County ---")
        transform_teen_births_county.run()

        print("\n--- Teen Births Race ---")
        transform_teen_births_race.run()

        print("\n--- Teen Births Trends ---")
        transform_teen_births_trends.run()

        print("\n--- Telemedicine COVID ---")
        transform_telemedicine_covid.run()

        print("\n--- Tobacco Legislation Tax ---")
        transform_tobacco_legislation_tax.run()

        print("\n--- Tobacco Preemption ---")
        transform_tobacco_preemption.run()

        print("\n--- Tobacco Smokefree Indoor ---")
        transform_tobacco_smokefree_indoor.run()

        print("\n--- Vital Statistics Monthly ---")
        transform_vital_statistics_monthly.run()

        print("\n--- Wastewater COVID Concentration ---")
        transform_wastewater_covid_concentration.run()

        print("\n--- Wastewater COVID Metrics ---")
        transform_wastewater_covid_metrics.run()

        print("\n--- Wastewater Public ---")
        transform_wastewater_public.run()

        print("\n--- Weekly Deaths Age ---")
        transform_weekly_deaths_age.run()

        print("\n--- Weekly Deaths Cause ---")
        transform_weekly_deaths_cause.run()

        print("\n--- WIC Obesity ---")
        transform_wic_obesity.run()

        print("\n--- Youth Access Legislation ---")
        transform_youth_access_legislation.run()

        print("\n--- Youth Nutrition Obesity ---")
        transform_youth_nutrition_obesity.run()

        print("\n--- YRBS Obesity ---")
        transform_yrbs_obesity.run()


if __name__ == "__main__":
    main()
