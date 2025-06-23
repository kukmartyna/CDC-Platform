import requests


def create_connector(connector_config):
    response = requests.post('http://connect:8083/connectors/', json=connector_config,
                  headers={'Content-Type': 'application/json'})
    response.raise_for_status()
    print(f"Successfully created connector {connector_config['name']}.")


def update_connector(connector_name, connector_config):
    try:
        response = requests.put(f'http://connect:8083/connectors/{connector_name}/config', json=connector_config,
                     headers={'Content-Type': 'application/json'})
        response.raise_for_status()
        print(f"Successfully updated connector {connector_name}.")
    except:
        raise Exception(f"Connector {connector_name} not found!")


def update_or_create_connector(connector_config):
    try:
        update_connector(connector_config['name'], connector_config['config'])
    except:
        create_connector(connector_config)
