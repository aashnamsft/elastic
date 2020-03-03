import os
import subprocess

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

from azure.common.credentials import ServicePrincipalCredentials
def get_credentials(tenant, client_id, secret):
    credentials = ServicePrincipalCredentials(
        tenant = tenant,
        client_id = client_id,
        secret = secret
    )

    return credentials


# Create resource group
def create_resource_group(resource_group_client, rg_name, location):
    resource_group_params = { 'location':location }
    resource_group_result = resource_group_client.resource_groups.create_or_update(
        rg_name,
        resource_group_params
    )



# Create availability set
def create_availability_set(compute_client, rg_name, location, avset_name):
    avset_params = {
        'location': location,
        'sku': { 'name': 'Aligned' },
        'platform_fault_domain_count': 3
    }
    availability_set_result = compute_client.availability_sets.create_or_update(
        rg_name,
        avset_name,
        avset_params
    )



# Create Public IP
def create_public_ip_address(network_client, rg_name, location, ip_name):
    public_ip_addess_params = {
        'location': location,
        'public_ip_allocation_method': 'Dynamic'
    }
    creation_result = network_client.public_ip_addresses.create_or_update(
        rg_name,
        ip_name,
        public_ip_addess_params
    )

    return creation_result.result()



# Create VNET
def create_vnet(network_client, rg_name, location, vnet_name):
    vnet_params = {
        'location': location,
        'address_space': {
            'address_prefixes': ['10.0.0.0/16']
        }
    }
    creation_result = network_client.virtual_networks.create_or_update(
        rg_name,
        vnet_name,
        vnet_params
    )
    return creation_result.result()

# Create a Network Security Group and open ports 29876,29877 for Batch
from azure.mgmt.network.models import NetworkSecurityGroup
from azure.mgmt.network.models import SecurityRule, SecurityRuleAccess, SecurityRuleDirection, SecurityRuleProtocol

def create_nsg(network_client, rg_name, location, nsg_name):
    params_create = NetworkSecurityGroup(
            location=location,
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

    result_create_NSG = network_client.network_security_groups.create_or_update(
            rg_name,
            nsg_name,
            params_create,
        )

    return result_create_NSG.result()

# Create subnet
def create_subnet(network_client, rg_name, vnet_name, subnet_name, nsg_obj):
    subnet_params = {
        'address_prefix': '10.0.0.0/24',
        'network_security_group' : nsg_obj
    }
    creation_result = network_client.subnets.create_or_update(
        rg_name,
        vnet_name,
        subnet_name,
        subnet_params
    )

    return creation_result.result()



# Create NIC
def create_nic(network_client, rg_name, location, vnet_name, subnet_name, ip_name, ipconfig_name, nic_name):
    subnet_info = network_client.subnets.get(
        rg_name,
        vnet_name,
        subnet_name
    )
    publicIPAddress = network_client.public_ip_addresses.get(
        rg_name,
        ip_name
    )
    nic_params = {
        'location': location,
        'ip_configurations': [{
            'name': ipconfig_name,
            'public_ip_address': publicIPAddress,
            'subnet': {
                'id': subnet_info.id
            }
        }]
    }
    creation_result = network_client.network_interfaces.create_or_update(
        rg_name,
        nic_name,
        nic_params
    )

    return creation_result.result()


# Create VM
def create_vm(network_client, compute_client, rg_name, location, nic_name, vm_name, sku):
    nic = network_client.network_interfaces.get(
        rg_name,
        nic_name
    )
#    avset = compute_client.availability_sets.get(
#        rg_name,
#        avset_name
#    )
    vm_parameters = {
        'location': location,
        'os_profile': {
            'computer_name': vm_name,
            'admin_username': 'azureuser',
            'admin_password': 'Azure12345678'
        },
        'hardware_profile': {
            'vm_size': sku
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
    creation_result = compute_client.virtual_machines.create_or_update(
        rg_name,
        vm_name,
        vm_parameters
    )

    return creation_result.result()

# Get information about VM
def get_vm(compute_client, rg_name, vm_name):
    vm = compute_client.virtual_machines.get(rg_name, vm_name, expand='instanceView')
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
    for stat in vm_result.instance_view.vm_agent.statuses:
        print("    code: ", stat.code)
        print("    displayStatus: ", stat.display_status)
        print("    message: ", stat.message)
        print("    time: ", stat.time)
    print("\ndisks");
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



def setup_etcd(compute_client, rg_name, vm_name):
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

    ext_poller = compute_client.virtual_machine_extensions.create_or_update(
        rg_name,
        vm_name,
        ext_name,
        params_create,
    )

    ext = ext_poller.result()

# Ping port 2379 to validate etcd setup
def verify_etcd(network_client, rg_name, ip_name):
    public_ip_address = network_client.public_ip_addresses.get(rg_name, ip_name)
    curl_cmd = ["curl -L http://{}:2379/version".format(public_ip_address.ip_address)]
    run_commands(curl_cmd)

# Delete resources
def delete_resources(resource_group_client, rg_name):
    resource_group_client.resource_groups.delete(rg_name)

# submit parallel single node jobs
from azureml.widgets import RunDetails
def submit_job(experiment, estimator, max_nodes):
    for node in range(0, max_nodes):
        pet_run = experiment.submit(estimator)
        RunDetails(pet_run).show()