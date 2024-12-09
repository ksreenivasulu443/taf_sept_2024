import yaml

with open('/Users/admin/PycharmProjects/pytest_framework/tests/table1/config.yml','r') as f:
    config = yaml.safe_load(f)
    print(config)