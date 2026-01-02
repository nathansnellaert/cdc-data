#!/usr/bin/env python3
"""Main orchestrator for CDC Data integration.

Runs each transform in a subprocess to avoid memory accumulation.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from tqdm import tqdm

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment

# List of all transforms (module names under transforms/)
TRANSFORMS = [
    "abcs_group_a_strep",
    "abcs_group_b_strep",
    "abcs_meningitis",
    "abcs_pneumococcal",
    "adult_obesity_trends",
    "age_adjusted_death_rates",
    "anxiety_depression",
    "birth_fertility_rates",
    "birth_indicators_quarterly",
    "birth_rates_unmarried",
    "breastfeeding_nis",
    "brfss_obesity",
    "brfss_prevalence",
    "child_health_conditions",
    "child_obesity_trends",
    "childhood_mortality",
    "county_drug_overdose_deaths",
    "covid_death_rates_monthly",
    "covid_deaths_age_race",
    "covid_deaths_county",
    "covid_deaths_county_race",
    "covid_deaths_demographics",
    "covid_deaths_hhs_region",
    "covid_deaths_hospital_region",
    "covid_deaths_jurisdiction",
    "covid_deaths_place",
    "covid_deaths_race_distribution",
    "covid_deaths_sex_age",
    "covid_deaths_state",
    "covid_deaths_youth",
    "covid_hospitalizations",
    "covid_hospitalizations_monthly",
    "covid_test_positivity",
    "covid_variant_proportions",
    "covid_variant_weekly",
    "covidnet_hospitalizations",
    "deaths_percent_respiratory",
    "deaths_race_ethnicity",
    "drug_overdose_deaths",
    "drug_overdose_rates",
    "drug_overdose_specific",
    "drug_poisoning_county",
    "drug_poisoning_modeled",
    "drug_poisoning_state",
    "ecig_licensure",
    "ecig_smokefree_indoor",
    "ed_visit_trends",
    "ed_visits_respiratory",
    "excess_deaths",
    "excess_deaths_causes",
    "flu_pneumonia_covid_deaths",
    "hai_cdi",
    "hospital_drug_use",
    "infant_mortality_quarterly",
    "leading_causes_death",
    "life_expectancy",
    "maternal_deaths",
    "mental_health_care",
    "monthly_deaths_causes",
    "natality_measures",
    "nchs_drug_poisoning_state",
    "nhanes_dietary",
    "nhis_adult_health",
    "nhis_vision",
    "nndss_weekly",
    "outbreak_nors",
    "physical_activity_acs",
    "provisional_drug_overdose",
    "quarterly_death_rates",
    "respiratory_hospitalizations_combined",
    "respiratory_vaccination",
    "rsv_hospitalizations",
    "rsv_hospitalizations_weekly",
    "rsv_test_positivity",
    "suicide_death_rates",
    "teen_births_county",
    "teen_births_race",
    "teen_births_trends",
    "telemedicine_covid",
    "tobacco_legislation_tax",
    "tobacco_preemption",
    "tobacco_smokefree_indoor",
    "vital_statistics_monthly",
    "wastewater_covid_concentration",
    "wastewater_covid_metrics",
    "wastewater_public",
    "weekly_deaths_age",
    "weekly_deaths_cause",
    "wic_obesity",
    "youth_access_legislation",
    "youth_nutrition_obesity",
    "yrbs_obesity",
]


def run_transform_subprocess(transform_name: str) -> bool:
    """Run a single transform in a subprocess to isolate memory."""
    cmd = [
        sys.executable, "-c",
        f"from transforms.{transform_name}.main import run; run()"
    ]

    try:
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=600,  # 10 min per transform
            env={**os.environ}
        )

        if result.stdout:
            # Only print if there's meaningful output
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    print(f"  {line}")

        if result.returncode != 0:
            if result.stderr:
                print(f"  Error: {result.stderr.strip()[:200]}")
            return False

        return True

    except subprocess.TimeoutExpired:
        print(f"  Timeout after 10 minutes")
        return False
    except Exception as e:
        print(f"  Failed: {e}")
        return False


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

        # Import ingest modules only when needed
        from ingest import datasets as ingest_datasets
        from ingest import health_indicators as ingest_health_indicators
        from ingest import raw_data as ingest_raw_data

        print("\n--- Datasets ---")
        ingest_datasets.run()

        print("\n--- Health Indicators ---")
        ingest_health_indicators.run()

        print("\n--- Raw Data (Selected Datasets) ---")
        ingest_raw_data.run()

    if should_transform:
        print(f"\n=== Phase 2: Transform ({len(TRANSFORMS)} datasets) ===")

        successful = []
        failed = []

        for transform in tqdm(TRANSFORMS, desc="Datasets"):
            success = run_transform_subprocess(transform)
            if success:
                successful.append(transform)
            else:
                failed.append(transform)

        print(f"\n✓ Completed: {len(successful)}/{len(TRANSFORMS)}")
        if failed:
            print(f"✗ Failed ({len(failed)}): {', '.join(failed[:5])}{'...' if len(failed) > 5 else ''}")


if __name__ == "__main__":
    main()
