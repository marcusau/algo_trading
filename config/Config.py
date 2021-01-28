import yaml
from pathlib import Path
import sys
from munch import munchify

current_path=Path().resolve()
project_path=current_path.parent
config_path=project_path/'config'
db_path=project_path / 'db_setting'
data_sources_path=project_path/ 'data_file'

sys.path.append(config_path)
sys.path.append(project_path)
sys.path.append(db_path)
sys.path.append(data_sources_path)


versose=False


data_source_yaml_filename= 'data_source_api.yaml'
data_source_yaml_file=config_path / data_source_yaml_filename
with open(data_source_yaml_file) as file:
    data_source_config=munchify(yaml.load(file, Loader=yaml.FullLoader))


db_yaml_filename= 'db.yaml'
db_yaml_file=config_path / db_yaml_filename
with open(db_yaml_file) as file:
    db_config=munchify(yaml.load(file, Loader=yaml.FullLoader))


if versose:
    print(data_source_config)
    print('\n')
    print(db_config.schema['usstock'])


