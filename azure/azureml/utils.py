import os
import subprocess

# Azure libraries
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute.models import DiskCreateOption
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.network.models import NetworkSecurityGroup
from azure.mgmt.network.models import SecurityRule, SecurityRuleAccess, SecurityRuleDirection, SecurityRuleProtocol

# AzureML libraries
import azureml.core
from azureml.core import Experiment, Workspace, Run
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException
from azureml.widgets import RunDetails

# This method runs all commands in a separate
# process and returns the output
def run_commands(cmds):
    output = []
    
    for cmd in cmds:
        print("Running {}".format(cmd))
        process = subprocess.Popen(
            cmd,
            universal_newlines=True,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=os.environ,
        )
        for line in process.stdout:
            print(line)
            output.append(line)
        for err in process.stderr:
            print(err)
    return output

def get_credentials(tenant, client_id, secret):
    credentials = ServicePrincipalCredentials(
        tenant = tenant,
        client_id = client_id,
        secret = secret
    )

    return credentials

class ElasticRun:
    def __init__(self, tenant, client_id, secret, subscription_id):
        self.tenant = tenant
        self.client_id = client_id
        self.secret = secret
        self.subscription_id = subscription_id
        self.num_nodes = 0
        
        # Generate user service principal credentials
        self.credentials = get_credentials(tenant, client_id, secret)

        # Create ARM resource client
        self.resource_group_client = ResourceManagementClient(self.credentials, self.subscription_id)
        self.network_client = NetworkManagementClient(self.credentials, self.subscription_id)
        self.compute_client = ComputeManagementClient(self.credentials, self.subscription_id)


    def init_resource_group(self, rg_name, location):
        self.rg_name = rg_name
        self.location = location

    # Create resource group
    def create_resource_group(self):
        resource_group_params = { 'location':self.location }
        resource_group_result = self.resource_group_client.resource_groups.create_or_update(
            self.rg_name,
            resource_group_params
        )


    def init_availability_set(self, avset_name):
        self.avset_name = avset_name

    # Create availability set
    def create_availability_set(self):
        avset_params = {
            'location': self.location,
            'sku': { 'name': 'Aligned' },
            'platform_fault_domain_count': 3
        }
        availability_set_result = self.compute_client.availability_sets.create_or_update(
            self.rg_name,
            self.avset_name,
            avset_params
        )


    def init_network_resources(self, ip_name, vnet_name, nsg_name, subnet_name, nic_name, ipconfig_name):
        self.ip_name = ip_name
        self.vnet_name = vnet_name
        self.nsg_name = nsg_name
        self.subnet_name = subnet_name
        self.nic_name = nic_name
        self.ipconfig_name = ipconfig_name

    # Create Public IP
    def create_public_ip_address(self):
        print("Creating Public IP Address")
        public_ip_addess_params = {
            'location': self.location,
            'public_ip_allocation_method': 'Dynamic'
        }
        creation_result = self.network_client.public_ip_addresses.create_or_update(
            self.rg_name,
            self.ip_name,
            public_ip_addess_params
        )
        return creation_result.result()

    # Create VNET
    def create_vnet(self):
        print("Creating VNET")
        vnet_params = {
            'location': self.location,
            'address_space': {
                'address_prefixes': ['10.0.0.0/16']
            }
        }
        creation_result = self.network_client.virtual_networks.create_or_update(
            self.rg_name,
            self.vnet_name,
            vnet_params
        )
        return creation_result.result()

    # Create a Network Security Group and open ports 29876,29877 for Batch
    def create_nsg(self):
        print("Creating NSG")
        params_create = NetworkSecurityGroup(
                location=self.location,
                security_rules=[
                    SecurityRule(
                        name='Port_29876-29877',
                        access=SecurityRuleAccess.allow,
                        description='Batch Node Management',
                        destination_address_prefix="*",
                        destination_port_range='29876-29877',
                        direction=SecurityRuleDirection.inbound,
                        priority=1040,
                        protocol=SecurityRuleProtocol.tcp,
                        source_address_prefix='BatchNodeManagement',
                        source_port_range="*",
                    ),
                ],
            )

        result_create_NSG = self.network_client.network_security_groups.create_or_update(
                self.rg_name,
                self.nsg_name,
                params_create,
            )
        return result_create_NSG.result()

    # Create subnet
    def create_subnet(self, nsg_obj):
        print("Creating Subnet")
        subnet_params = {
            'address_prefix': '10.0.0.0/24',
            'network_security_group' : nsg_obj
        }
        creation_result = self.network_client.subnets.create_or_update(
            self.rg_name,
            self.vnet_name,
            self.subnet_name,
            subnet_params
        )
        return creation_result.result()

    # Create NIC
    def create_nic(self):
        print("Creating NIC")
        subnet_info = self.network_client.subnets.get(
            self.rg_name,
            self.vnet_name,
            self.subnet_name
        )
        publicIPAddress = self.network_client.public_ip_addresses.get(
            self.rg_name,
            self.ip_name
        )
        nic_params = {
            'location': self.location,
            'ip_configurations': [{
                'name': self.ipconfig_name,
                'public_ip_address': publicIPAddress,
                'subnet': {
                    'id': subnet_info.id
                }
            }]
        }
        creation_result = self.network_client.network_interfaces.create_or_update(
            self.rg_name,
            self.nic_name,
            nic_params
        )
        return creation_result.result()

    # Create required Network resources
    def create_network_resources(self):
        self.create_public_ip_address()
        self.create_vnet()
        nsg_obj = self.create_nsg()
        self.create_subnet(nsg_obj)
        self.create_nic()

    # VM details for etcd setup
    def init_etcd_vm(self, vm_name, vm_size):
        self.vm_name = vm_name
        self.vm_size = vm_size

    # Create VM for etcd
    def create_etcd_vm(self):
        print("Creating VM for ETCD")
        nic = self.network_client.network_interfaces.get(
            self.rg_name,
            self.nic_name
        )
    #    avset = self.compute_client.availability_sets.get(
    #        self.rg_name,
    #        self.avset_name
    #    )
        vm_parameters = {
            'location': self.location,
            'os_profile': {
                'computer_name': self.vm_name,
                'admin_username': 'azureuser',
                'admin_password': 'Azure12345678'
            },
            'hardware_profile': {
                'vm_size': self.vm_size
            },
            'storage_profile': {
                'image_reference': {
                    'publisher': 'Canonical',
                    'offer': 'UbuntuServer',
                    'sku': '16.04.0-LTS',
                    'version': 'latest'
                }
            },
            'network_profile': {
                'network_interfaces': [{
                    'id': nic.id
                }]
            },
        }
    #        'availability_set': {
    #            'id': avset.id
    #        }
    #    }
        creation_result = self.compute_client.virtual_machines.create_or_update(
            self.rg_name,
            self.vm_name,
            vm_parameters
        )
        return creation_result.result()

    # Run custom script extension to setup etcd
    def setup_etcd_vm(self):
        print("Setting up ETCD")
        ext_type_name = 'CustomScriptForLinux'
        ext_name = 'installetcd'
        params_create = {
            'location': 'SouthCentralUS',
            'publisher': 'Microsoft.OSTCExtensions',
            'virtual_machine_extension_type': ext_type_name,
            'type_handler_version': '1.5',
            'auto_upgrade_minor_version': True,
            'settings': {
                'fileUris': ["https://gist.githubusercontent.com/raviskolli/2e76108cfafb55ac50650c77ad1c9cc6/raw/12cc7fb0fcd53b5d7c4d8c2f04ca3ead5a1a3c43/install-etcd.sh"],
                'commandToExecute': "sh install-etcd.sh"
                }
        }

        ext_poller = self.compute_client.virtual_machine_extensions.create_or_update(
            self.rg_name,
            self.vm_name,
            ext_name,
            params_create,
        )
        ext = ext_poller.result()

    def create_setup_etcd_vm(self):
        self.create_etcd_vm()
        self.setup_etcd_vm()

    # Get information about VM
    def get_etcd_vm(self):
        vm = self.compute_client.virtual_machines.get(self.rg_name, self.vm_name, expand='instanceView')
        print("hardwareProfile")
        print("   vmSize: ", vm.hardware_profile.vm_size)
        print("\nstorageProfile")
        print("  imageReference")
        print("    publisher: ", vm.storage_profile.image_reference.publisher)
        print("    offer: ", vm.storage_profile.image_reference.offer)
        print("    sku: ", vm.storage_profile.image_reference.sku)
        print("    version: ", vm.storage_profile.image_reference.version)
        print("  osDisk")
        print("    osType: ", vm.storage_profile.os_disk.os_type.value)
        print("    name: ", vm.storage_profile.os_disk.name)
        print("    createOption: ", vm.storage_profile.os_disk.create_option.value)
        print("    caching: ", vm.storage_profile.os_disk.caching.value)
        print("\nosProfile")
        print("  computerName: ", vm.os_profile.computer_name)
        print("  adminUsername: ", vm.os_profile.admin_username)
        print("  provisionVMAgent: {0}".format(vm.os_profile.windows_configuration.provision_vm_agent))
        print("  enableAutomaticUpdates: {0}".format(vm.os_profile.windows_configuration.enable_automatic_updates))
        print("\nnetworkProfile")
        for nic in vm.network_profile.network_interfaces:
            print("  networkInterface id: ", nic.id)
        print("\nvmAgent")
        print("  vmAgentVersion", vm.instance_view.vm_agent.vm_agent_version)
        print("    statuses")
        for stat in vm.instance_view.vm_agent.statuses:
            print("    code: ", stat.code)
            print("    displayStatus: ", stat.display_status)
            print("    message: ", stat.message)
            print("    time: ", stat.time)
        print("\ndisks")
        for disk in vm.instance_view.disks:
            print("  name: ", disk.name)
            print("  statuses")
            for stat in disk.statuses:
                print("    code: ", stat.code)
                print("    displayStatus: ", stat.display_status)
                print("    time: ", stat.time)
        print("\nVM general status")
        print("  provisioningStatus: ", vm.provisioning_state)
        print("  id: ", vm.id)
        print("  name: ", vm.name)
        print("  type: ", vm.type)
        print("  location: ", vm.location)
        print("\nVM instance status")
        for stat in vm.instance_view.statuses:
            print("  code: ", stat.code)
            print("  displayStatus: ", stat.display_status)

    # Ping port 2379 to validate etcd setup
    def verify_etcd(self):
        public_ip_address = self.network_client.public_ip_addresses.get(self.rg_name, self.ip_name)
        curl_cmd = ["curl -L http://{}:2379/version".format(public_ip_address.ip_address)]
        run_commands(curl_cmd)

    # Delete resources
    def delete_resources(self):
        self.resource_group_client.resource_groups.delete(self.rg_name)


    # Azure Machine Learning helper functions

    # AzureML workspace setup
    def get_workspace(self, workspace_name):
        try:
            self.ws = Workspace(subscription_id = self.subscription_id,
            resource_group = self.rg_name,
            workspace_name = workspace_name)
            # write the details of the workspace to a configuration file to the notebook library
            #ws.write_config()
            print("Workspace configuration succeeded. Skip the workspace creation steps below")
            return self.ws
        except:
            print("Workspace not accessible. Change your parameters or create a new workspace using create_workspace")

    def create_workspace(self, workspace_name):
        # Create the workspace using the specified parameters
        self.ws = Workspace.create(name = workspace_name,
                            subscription_id = self.subscription_id,
                            resource_group = self.rg_name, 
                            location = self.location,
                            create_resource_group = True,
                            exist_ok = True)
        self.ws.get_details()
        return self.ws

    # AML Compute Cluster Setup
    def create_amlcompute_cluster(self, pet_cluster_name, min_nodes, max_nodes, vm_size):
        self.min_nodes = min_nodes
        self.max_nodes = max_nodes

        # Verify that the cluster doesn't exist already
        try:
            self.pet_compute_target = ComputeTarget(workspace=self.ws, name=pet_cluster_name)
            print('Found existing compute target.')
        except ComputeTargetException:
            print('Creating a new compute target...')
            compute_config = AmlCompute.provisioning_configuration(vm_size=vm_size,
                                                                min_nodes=min_nodes,
                                                                max_nodes=max_nodes,
                                                                vnet_name=self.vnet_name,
                                                                vnet_resourcegroup_name=self.rg_name,
                                                                subnet_name=self.subnet_name)
            
            # create the cluster
            self.pet_compute_target = ComputeTarget.create(self.ws, pet_cluster_name, compute_config)
            self.pet_compute_target.wait_for_completion(show_output=True)

        # Use the 'status' property to get a detailed status for the current cluster. 
        #print(self.pet_compute_target.status.serialize())
        return self.pet_compute_target
    
    # Setup Azure Machine Learning Experiment
    def create_experiment(self, experiment_name):
        self.pet_experiment = Experiment(self.ws, name=experiment_name)
        
    # submit parallel single node jobs
    def submit_job(self, estimator, num_nodes):
        assert self.num_nodes == 0, "Job already in progress, node count can be scaled using scale_job() method"
        assert num_nodes >= self.min_nodes, "Node count should be greater than or equal to Minimum Node count"
        assert num_nodes <= self.max_nodes, "Node count should be lesser than or equal to Maximum Node count"
        self.estimator = estimator
        self.num_nodes = num_nodes
        for node in range(0, num_nodes):
            pet_run = self.pet_experiment.submit(estimator)
            RunDetails(pet_run).show()

    def scale_job(self, num_nodes):
        assert num_nodes <= self.max_nodes, "Node count should be lesser than or equal to Maximum Node count"
        for node in range(self.num_nodes, num_nodes):
            pet_run = self.pet_experiment.submit(self.estimator)
            RunDetails(pet_run).show()
        self.num_nodes = num_nodes