import yaml
from pipeline import GA1TX8
from IPython.display import display


def main():
    with open('/Users/xuanedx1/github/outage-data-scraper/app/pipeline/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        base_file_path = config['globals']['local_base_file_path']

    # Instantiate a pipeline object for each provider
    for provider in config['providers']:
        pipeline = GA1TX8(provider, base_file_path)
        pipeline.standardize()
        print(pipeline._data)