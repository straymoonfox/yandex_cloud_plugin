import requests
import jwt
import time
import os
from requests.exceptions import RequestException
from ansible.plugins.inventory import BaseInventoryPlugin, to_safe_group_name
from ansible.errors import AnsibleError

DOCUMENTATION = '''
    name: yandex_cloud
    plugin_type: inventory
    short_description: Yandex.Cloud inventory source
    requirements:
        - python >= 3.6
'''

class InventoryModule(BaseInventoryPlugin):
    NAME = 'devim.yandex_cloud_plugin.yandex_cloud'

    API_CLOUDS_URL = 'https://resource-manager.api.cloud.yandex.net/resource-manager/v1/clouds/'
    API_FOLDERS_URL = 'https://resource-manager.api.cloud.yandex.net/resource-manager/v1/folders/'
    API_INSTANCES_URL = 'https://compute.api.cloud.yandex.net/compute/v1/instances/'

    def verify_file(self, path):
        """
        Verify plugin configuration file and mark this plugin active
        Args:
            path: Path of configuration YAML file
        Returns: True if everything is correct, else False
        """
        valid = False
        if super(InventoryModule, self).verify_file(path):
            if path.endswith(('yandex_cloud.yml', 'yandex_cloud.yaml')):
                valid = True

        return valid


    def parse(self, inventory, loader, path, cache=True):
        """
        Parse the inventory file and populate the inventory object with hosts and groups.
        """
        super(InventoryModule, self).parse(inventory, loader, path)

        config = self._read_config_data(path)
        service_account_id = os.getenv('YC_SERVICE_ACCOUNT_ID') or config['api']['service_account_id']
        if not service_account_id:
            raise AnsibleError('Service account ID not found')
        private_key = os.getenv('YC_PRIVATE_KEY') or config['api']['private_key']
        if not private_key:
            raise AnsibleError('Private key not found')
        key_id = os.getenv('YC_KEY_ID') or config['api']['key_id']
        if not key_id:
            raise AnsibleError('Key id not found')
        iam_token = self.get_iam_token(service_account_id, private_key, key_id)
        if not iam_token:
            raise AnsibleError('IAM token not found')

        cloud_ids = config['api']['cloud_ids']

        headers = {'Authorization': f'Bearer {iam_token}'}

        for cloud_id in cloud_ids:
            # Get cloud name
            cloud_name = self.get_cloud_name(cloud_id, headers)
            self.create_cloud_group(cloud_name)

            # Get folders
            folders = self.get_folders(cloud_id, headers)

            for folder in folders:
                folder_id = folder['id']
                # Get folder name
                folder_name = self.get_folder_name(folder_id, headers)
                self.create_folder_group(folder_name, cloud_name)

                # Get instances
                instances = self.get_instances(folder_id, headers)

                if instances:
                    for instance in instances:
                        # Process instance
                        self.process_instance(instance, folder_name)

    def get_iam_token(self, service_account_id, private_key,key_id):
        """
        Get an IAM token for the specified service account.
        """
        now = int(time.time())
        payload = {
            'aud': 'https://iam.api.cloud.yandex.net/iam/v1/tokens',
            'iss': service_account_id,
            'iat': now,
            'exp': now + 360
        }
        encoded_token = jwt.encode(
            payload, 
            private_key.replace(r'\n', '\n'), 
            algorithm='PS256',
            headers={'kid': key_id}
        )
        response = requests.post('https://iam.api.cloud.yandex.net/iam/v1/tokens', json={'jwt':encoded_token})
        return response.json()['iamToken']

    def api_get_request(self, url, headers):
        """
        Make an API GET request with the specified URL and headers.
        """
        with requests.Session() as session:
            try:
                response = session.get(url, headers=headers)
                response.raise_for_status()
            except RequestException as err:
                raise AnsibleError(f"An error occurred: {err}")

        return response.json()

    def get_cloud_name(self, cloud_id, headers):
        """
        Get the name of the cloud by its ID.
        """
        cloud_url = self.API_CLOUDS_URL + cloud_id
        response = self.api_get_request(cloud_url, headers)
        cloud_name = response['name']
        return to_safe_group_name(cloud_name)

    def create_cloud_group(self, cloud_name):
        """
        Create a group for the cloud.
        """
        self.inventory.add_group(cloud_name)

    def get_folders(self, cloud_id, headers):
        """
        Get the list of folders for the given cloud ID.
        """
        folders_url = self.API_FOLDERS_URL + f'?cloudId={cloud_id}'
        response = self.api_get_request(folders_url, headers)
        return response.get('folders', [])

    def get_folder_name(self, folder_id, headers):
        """
        Get the name of the folder by its ID.
        """
        folder_url = self.API_FOLDERS_URL + folder_id
        response = self.api_get_request(folder_url, headers)
        folder_name = response['name']
        return to_safe_group_name(folder_name)

    def create_folder_group(self, folder_name, cloud_name):
        """
        Create a group for the folder and add it as a child of the cloud group.
        """
        self.inventory.add_group(folder_name)
        self.inventory.add_child(cloud_name, folder_name)

    def get_instances(self, folder_id, headers):
        """
        Get the list of instances for the given folder ID.
        """
        instances_url = self.API_INSTANCES_URL + f'?folderId={folder_id}'
        response = self.api_get_request(instances_url, headers)
        return response.get('instances', [])

    def process_instance(self, instance, folder_name):
        """
        Process instance information, add the host to the inventory, and create label groups if present.
        """
        name = instance['name']
        ip = instance['networkInterfaces'][0]['primaryV4Address']['address']

        self.inventory.add_host(name)
        self.inventory.set_variable(name, 'ansible_host', ip)

        if 'labels' in instance:
            for label in instance['labels']:
                label = to_safe_group_name(label)
                label_group_folder = f"{folder_name}_{label}"
                self.create_label_group(label_group_folder, folder_name)
                self.add_host_to_group(name, label_group_folder)
        else:
            self.add_host_to_group(name, folder_name)

    def create_label_group(self, label_group_folder, folder_name):
        """
        Create a group for the label and add it as a child of the folder group.
        """
        self.inventory.add_group(label_group_folder)
        self.inventory.add_child(folder_name, label_group_folder)

    def add_host_to_group(self, host, group):
        """
        Add the host to the specified group.
        """
        self.inventory.add_child(group, host)
