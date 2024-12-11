import yaml

def load_yaml_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


config = load_yaml_config('/home/developer/keys/project-keys/colab-settings.yaml')
print(config)