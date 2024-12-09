import yaml


with open('/Users/admin/PycharmProjects/taf/tests/table3/config.yml','r') as f:
    config_data = yaml.safe_load(f)
    print("source config", config_data['source'])
    print("target config", config_data['target'])