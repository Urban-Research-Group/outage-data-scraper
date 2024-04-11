import yaml
from pipeline import CA1
from IPython.display import display


def main():
    with open('/Users/xuanedx1/github/outage-data-scraper/app/pipeline/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        base_file_path = config['globals']['LOCAL_FILE_BASE_PATH']

    # Instantiate a pipeline object for each provider
    for provider in config['providers']:
        pipeline = CA1(provider, base_file_path)
        pipeline.standardize(method='timegap')
        print(pipeline._data)
        
if __name__ == "__main__":
    main()