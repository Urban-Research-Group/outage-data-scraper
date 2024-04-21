import yaml
import pandas as pd
from pipeline import CA1, GA1TX8, GA4TX5


def main():
    with open('/Users/xuanedx1/github/outage-data-scraper/app/pipeline/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        base_file_path = config['globals']['LOCAL_FILE_BASE_PATH']

    # Instantiate a pipeline object for each provider
    for provider in config['providers']:
        pipeline = GA4TX5(provider, base_file_path)
        # std = pipeline.to_incident_level(
        #     identifers=['outage_id', 'latitude', 'longitude'], 
        #     method='timegap_seperation'
        #     )
        
        std = pipeline.to_geoarea_level()
                
        print(std)
        
        # std.to_csv('/Users/xuanedx1/github/outage-data-scraper/scripts/investor_owned_outage_level-v2.csv')
        
        
if __name__ == "__main__":
    main()