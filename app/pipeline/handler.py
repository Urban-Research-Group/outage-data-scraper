import yaml
from pipeline import CA1, GA1TX8


def main():
    with open('/Users/xuanedx1/github/outage-data-scraper/app/pipeline/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        base_file_path = config['globals']['LOCAL_FILE_BASE_PATH']

    # Instantiate a pipeline object for each provider
    for provider in config['providers']:
        pipeline = GA1TX8(provider, base_file_path)
        pipeline.standardize_new(geo_level='incident', 
                                 identifer='outage_id', 
                                 method='id_grouping')
        print(pipeline.get_dataframe())
        
if __name__ == "__main__":
    main()