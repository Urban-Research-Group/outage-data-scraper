

# Usage
config = {"parameter": "value",}
pipeline_a = DatasetATransformer(config)
pipeline_b = DatasetBTransformer(config)

# Assuming `data_a` and `data_b` are your raw datasets
transformed_a = pipeline_a.transform(data_a)
transformed_b = pipeline_b.transform(data_b)