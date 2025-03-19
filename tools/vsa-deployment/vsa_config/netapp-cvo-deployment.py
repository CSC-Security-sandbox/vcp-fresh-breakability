# Copyright (C) 2020 NetApp Inc.
# All rights reserved.

"""Creates a configuration for the compute instance and creates ordering of resource dependency."""

class TemplateError(Exception):
    """Class of type Exception to raise TemplateError."""
    pass

def _get_resource_from_context(context, resource_type):
    """Given a context and resource_type, retrieve the resource if it exists else raise an error."""
    resource = context.properties.get(resource_type)
    if not resource:
        raise TemplateError('Property "{resource}" not specified'.format(resource=resource_type))
    return resource

def get_labels(context):
    """Get the user-provided labels (if any)."""
    try:
        return _get_resource_from_context(context, 'labels')
    except:
        return dict()

def get_service_accounts(context):
    """Create service account entries."""
    service_accounts = list()
    # Append service account to list (note: 'scopes' set to cloud-plaform(same as console default))
    try:
        service_account_name = _get_resource_from_context(context, 'serviceAccount')
        # if no @ assume just name and add a default 'domain'
        if '@' not in service_account_name:
            service_account_name = service_account_name+'@'+ context.env['project'] +'.iam.gserviceaccount.com'
        scopes = list()
        scopes.append('https://www.googleapis.com/auth/cloud-platform')
        service_accounts.append({'email': service_account_name, 'scopes': scopes})
        return service_accounts
    except:
        return list()

def build_address_resource(context, resource_name, resource_type, description, purpose, subnet_project, subnet, ip_address=None):
    address_resource = {
        'name': resource_name,
        'type': resource_type,
        'properties': {
            'name': resource_name,
            'description': description,
            'addressType': 'INTERNAL',
            'purpose': purpose,
            'subnetwork': 'projects/' + subnet_project + '/regions/' + context.properties['region'] + '/subnetworks/' + subnet,
            'region': context.properties['region']
        }
    }

    if ip_address is not None:
        address_resource['properties']['address'] = ip_address

    return address_resource

def build_health_check_resource(context, resource_name, resource_type, description, port):
    health_check_resource = {
        'name': resource_name,
        'type': resource_type,
        'properties': {
            'name': resource_name,
            'description': description,
            'type': 'TCP',
            'checkIntervalSec': 5,
            'timeoutSec': 5,
            'unhealthyThreshold': 2,
            'healthyThreshold': 2,
            'tcpHealthCheck': {
                'port': port
            },
            'region': context.properties['region']
        }
    }

    return health_check_resource

def build_backend_service_resource(context, resource_name, resource_type, description, vpc_project, vpc_name,
                                   protocol, primary_backend, failover_backend, health_check_name):
    backend_service_resource = {
        'name': resource_name,
        'type': resource_type,
        'properties': {
            'name': resource_name,
            'description': description,
            'region': context.properties['region'],
            'network': 'projects/' + vpc_project + '/global/networks/' + vpc_name,
            'loadBalancingScheme': 'INTERNAL',
            'protocol': protocol,
            'backends': [
                {
                    'group': '$(ref.' + primary_backend + '.selfLink)',
                    'failover': False
                },
                {
                    'group': '$(ref.' + failover_backend + '.selfLink)',
                    'failover': True
                }
            ],
            'healthChecks': [
                '$(ref.' + health_check_name + '.selfLink)'
            ],
            'failoverPolicy': {
                'dropTrafficIfUnhealthy': True
            }
        }
    }
    if protocol == 'TCP':
        backend_service_resource['properties']['failoverPolicy'].update({'disableConnectionDrainOnFailover': True})

    return backend_service_resource

def build_forwarding_rule_resource(context, resource_name, resource_type, description, vpc_project, vpc_name,
                                   subnet, address_resource, protocol, backend_service_resource, enable_global_access=False, ip_address=None):
    forwarding_rule_resource = {
        'name': resource_name,
        'type': resource_type,
        'properties': {
            'name': resource_name,
            'description': description,
            'region': context.properties['region'],
            'network': 'projects/' + vpc_project + '/global/networks/' + vpc_name,
            'subnetwork': 'projects/' + vpc_project + '/regions/' + context.properties['region'] + '/subnetworks/' + subnet,
            'loadBalancingScheme': 'INTERNAL',
            'IPProtocol': protocol,
            'backendService': '$(ref.' + backend_service_resource + '.selfLink)',
            'allPorts': True,
            'allowGlobalAccess': enable_global_access
        }
    }

    if ip_address is not None:
        forwarding_rule_resource['properties']['IPAddress'] = ip_address
    else:
        forwarding_rule_resource['properties']['IPAddress'] = '$(ref.' + address_resource + '.address)'

    return forwarding_rule_resource

def build_pd_resource(context, resource_name, resource_type, labels, pd_type, size, zone1, is_shared=False, source_image=None, zone2=None):
    disk_resource = {
        'name': resource_name,
        'type': resource_type,
        'properties': {
            'name': resource_name,
            'labels': labels,
            'sizeGb': size
        }
    }
    # is multi-writer PD?
    if is_shared:
        disk_resource['properties']['multiWriter'] = True
    # is regional PD?
    if zone2 is not None:
        disk_resource['properties']['region'] = context.properties['region']
        disk_resource['properties']['replicaZones'] = [
            'projects/' + context.env['project'] + '/zones/' + zone1,
            'projects/' + context.env['project'] + '/zones/' + zone2
        ]
        disk_resource['properties']['type'] = 'projects/' + context.env['project'] + '/regions/' + context.properties['region'] + '/diskTypes/' + pd_type
    else:
        disk_resource['properties']['zone'] = zone1
        disk_resource['properties']['type'] = 'projects/' + context.env['project'] + '/zones/' + zone1 + '/diskTypes/' + pd_type
    # is boot disk?
    if source_image is not None:
        disk_resource['properties']['sourceImage'] = source_image

    return disk_resource

def get_pd_resource(disk_name, is_boot=False, disk_interface='SCSI'):
    disk_resource = {
        'type': 'PERSISTENT',
        'mode': 'READ_WRITE',
        'source': '$(ref.' + disk_name + '.selfLink)',
        'deviceName': disk_name,
        'boot': is_boot,
        'autoDelete': False,
        'interface': disk_interface
    }

    return disk_resource

def build_local_ssd_resources(context, vm_name, vm_zone, local_ssd_count, local_ssd_interface):
    """Build Local-SSD resources."""
    local_ssd_resources = list()
    separator = '-'
    if local_ssd_count is not 0:
        for index in range(local_ssd_count):
            local_ssd_suffix = 'localssd' + "%02d" % (index+1)
            local_ssd_name = separator.join([vm_name, 'disk', local_ssd_suffix])
            local_ssd_resources.append({
                'type': 'SCRATCH',
                'mode': 'READ_WRITE',
                'initializeParams': {
                    'diskType': 'projects/' + context.env['project'] + '/zones/' + vm_zone + '/diskTypes/local-ssd'
                },
                'deviceName': local_ssd_name,
                'boot': False,
                'autoDelete': True,
                'interface': local_ssd_interface
            })

    return local_ssd_resources

def build_sole_tenancy_node_template(context, resource_name, sole_tenant_node_group_name, resource_type, node_type):
    sole_tenancy_node_template_resource = {
        'name': resource_name,
        'type': resource_type,
        'properties': {
            'name': resource_name,
            'region': context.properties['region'],
            'nodeType': node_type,
            'nodeAffinityLabels': {
                'key': sole_tenant_node_group_name
            }
        }
    }

    return sole_tenancy_node_template_resource

def build_sole_tenancy_node_group(context, resource_name, sole_tenant_node_template_name, resource_type):
    sole_tenancy_node_group_resource = {
        'name': resource_name,
        'type': resource_type,
        'properties': {
            'name': resource_name,
            'region': context.properties['region'],
            'zone': context.properties['zone'],
            'nodeTemplate': '$(ref.' + sole_tenant_node_template_name + '.selfLink)',
            'autoscalingPolicy': {
                'mode': 'OFF',
                'minNodes': 1
            },
            'initialNodeCount': 1
        }
    }

    return sole_tenancy_node_group_resource

def get_flashcache_settings(context):
    """Get the flashcache settings."""
    count = 0
    interface = 'NVME'
    local_ssd_count_map = {
                            'c3-standard-8-lssd'  : 2,
                            'n2-standard-8'  : 8,
                            'n2-standard-16' : 8,
                            'n2-standard-32' : 8,
                            'n2-standard-48' : 8,
                            'n2-standard-64' : 8
                          }
    try:
        enable_flashcache = _get_resource_from_context(context, 'enableFlashCache')
        if enable_flashcache:
            count = local_ssd_count_map[context.properties['machineType']]
        # check if 'localSSD' object is provided in the config yaml
        local_ssd_obj = _get_resource_from_context(context, 'localSSD')
        count = local_ssd_obj['count']
        interface = local_ssd_obj['interface'].upper()
        return (count, interface)
    except:
        return (count, interface)

def build_instance_group_resource(resource_name, resource_type, description, zone):
    instance_group_resource = {
        'name': resource_name,
        'type': resource_type,
        'properties': {
            'name': resource_name,
            'description': description,
            'zone': zone
        }
    }

    return  instance_group_resource

def get_preemptible(context):
    """Get the preemptible setting."""
    try:
        return _get_resource_from_context(context, 'preemptible')
    except:
        return False

def get_enable_gvnic(context):
    """Get user provided enableGVNIC."""
    try:
        return _get_resource_from_context(context, 'enableGVNIC')
    except:
        return None

def get_min_cpu_platform(context):
    """Get user provided minCpuPlatform."""
    try:
        return _get_resource_from_context(context, 'minCpuPlatform')
    except:
        return None

def get_startup_script(context):
    """Get user provided startup script."""
    try:
        return _get_resource_from_context(context, 'startup-script')
    except:
        return None

def get_ha_deployment_obj(context):
    """Get the HA deployment object (if any)."""
    try:
        return _get_resource_from_context(context, 'haDeployment')
    except:
        return None

def get_data_nic_obj(context):
    """Get the HA deployment object (if any)."""
    try:
        return _get_resource_from_context(context, 'virtualNetworkInterfaceForData')
    except:
        return None

def get_sole_tenant_node_obj(context):
    """Get the Sole-tenant node object (if any)."""
    try:
        return _get_resource_from_context(context, 'soleTenantNode')
    except:
        return None

def get_object_value(object, key):
    """Get the value of an object (if exists)."""
    try:
        return object[key]
    except:
        return None

def get_intercluster_lif_ips(context):
    """Get user provided intercluster LIF IPs (one per node)"""
    try:
        return _get_resource_from_context(context, 'interclusterLifIps')
    except:
        return None

def get_iscsi_lif_ips(context):
    """Get user provided ISCSI LIF IPs (one per node)."""
    try:
        return _get_resource_from_context(context, 'iscsiLifIps')
    except:
        return None

def get_ilb_ips(context):
    """Get user provided ILB IPs (one per HA pair)"""
    try:
        return _get_resource_from_context(context, 'ilbIps')
    except:
        return None


def generate_config(context):
    """Generates configuration for NetApp Cloud Volumes ONTAP (CVO) deployment."""

    # constants
    node_mgmt_interface = 'nodemgmt'
    cluster_mgmt_interface = 'clustermgmt'
    intercluster_interface = 'intercluster'
    iscsi_interface = 'iscsi'
    nas_interface = 'nas'
    nic = 'nic0'
    data_nic = 'nic0'
    separator = '-'
    ip = 'ip'
    boot_disk_size_gb = '10'
    nvram_disk_size_gb = '500'
    core_disk_size_gb = '315'

    # resource types
    compute_address_v1 = 'compute.v1.address'
    compute_address_beta = 'compute.beta.address'
    compute_health_check_v1 = 'compute.v1.healthCheck'
    compute_health_check_beta = 'compute.beta.healthCheck'
    compute_backend_service_v1 = 'compute.v1.regionBackendService'
    compute_backend_service_beta = 'compute.beta.regionBackendService'
    compute_forwarding_rule_v1 = 'compute.v1.forwardingRule'
    compute_forwarding_rule_beta = 'compute.beta.forwardingRule'
    compute_disk_v1 = 'compute.v1.disk'
    compute_disk_beta = 'compute.beta.disk'
    compute_disk_alpha = 'gcp-types/compute-alpha:disks'
    compute_region_disk_v1 = 'gcp-types/compute-v1:regionDisks'
    compute_region_disk_alpha = 'gcp-types/compute-alpha:regionDisks'
    compute_instance_group_v1 = 'compute.v1.instanceGroup'
    compute_resource_policy_v1 = 'gcp-types/compute-v1:resourcePolicies'
    compute_instance_v1 = 'compute.v1.instance'
    compute_node_template_v1 = 'gcp-types/compute-v1:nodeTemplates'
    compute_node_group_v1 = 'gcp-types/compute-v1:nodeGroups'
    #sole-tenant template and Group
    node_template = 'node-template'
    node_group = 'node-group'
    # PD types
    pd_ssd = 'pd-ssd'
    pd_balanced = 'pd-balanced'
    # local-ssd interface types
    scsi = 'SCSI'
    nvme = 'NVME'
    # address purpose
    gce_endpoint = 'GCE_ENDPOINT'
    shared_loadbalancer_vip = 'SHARED_LOADBALANCER_VIP'
    # protocol
    tcp = 'TCP'
    udp = 'UDP'

    # set environment
    resources = list()
    deployment_name = context.env['deployment']
    project_name = context.env['project']
    deployment_type = "singlenode"
    isBlockDeployment = context.properties['isBlockDeployment']
    vnic0_obj = context.properties['virtualNetworkInterface0']
    nvlog_media_type = context.properties['nvLogMediaType']
    # vm1_name = separator.join([deployment_name, 'vm'])
    numberOfHaPairs = context.properties['numberOfHaPairs']
    vm1_name = separator.join([deployment_name, 'vm'])
    system_disks_count = 8
    totalDataDisksSizeInGB = context.properties['dataDisksStorageCapacityInGB']
    data_disk_size_gb = totalDataDisksSizeInGB/4
    root_disk_size_gb = data_disk_size_gb

    labels = get_labels(context)
    description = '\n'.join('{}: {}'.format(key, value) for key,value in labels.items())
    service_accounts = get_service_accounts(context)
    is_preemptible = get_preemptible(context)

    # hidden options (not listed in the schema)
    startup_script = get_startup_script(context)
    min_cpu_platform = get_min_cpu_platform(context)
    (local_ssd_count, local_ssd_interface) = get_flashcache_settings(context)
    enable_gvnic = get_enable_gvnic(context)
    nic_tier = "DEFAULT"
    nic_type = "VirtIO"
    if enable_gvnic:
        nic_tier = "TIER_1"
        nic_type = "GVNIC"

    # common metadata
    common_metadata_entries = [
        {
            'key': 'serial-port-enable',
            'value': 1
        },
        {
            'key': 'serial-port-logging-enable',
            'value': 'true'
        }
    ]
    # check if startup-script is provided
    if startup_script:
        common_metadata_entries.append({
            'key': 'startup-script',
            'value': context.imports[startup_script]
        })

    # extract/set project-id of VPC used to host vnic0
    vnic0_vpc_project = get_object_value(vnic0_obj, 'project')
    if not vnic0_vpc_project:
        vnic0_vpc_project = project_name

    # extract sourceImage property
    source_image_context = context.properties['sourceImage']
    source_image_project = get_object_value(source_image_context, 'project')
    if not source_image_project:
        source_image_project = project_name

    # extract IPs
    intercluster_lif_ips = get_intercluster_lif_ips(context)
    iscsi_lif_ips = get_iscsi_lif_ips(context)
    ilb_ips = get_ilb_ips(context)

    # extract zone
    vm1_zone = context.properties['zone']
    vm1_region = vm1_zone.rpartition("-")[0]
    if vm1_region != context.properties['region']:
        error_msg = "Supplied zone [%s] for VM1 does not belong to supplied region [%s]." % (vm1_zone, context.properties['region'])
        sys.exit(error_msg)

    totalNumberOfNodes = numberOfHaPairs * 2
    currentNodeCount = 0
    currentPartnerNodeCount = 0
    cluster_join_ip = ""
    for haIndex in range(numberOfHaPairs):
        currentHaIndex = haIndex + 1
        resourcePrefix = 'ha'+str(currentHaIndex)
        currentNodeCount = currentPartnerNodeCount + 1
        currentPartnerNodeCount = currentNodeCount + 1
        is_cluster_joining_node = "true" if currentNodeCount > 1 else "false"
        # check if HA deployment is requested
        ha_deployment_obj = get_ha_deployment_obj(context)
        if ha_deployment_obj:
            deployment_type = 'shared_ha'
            system_disks_count = 9
            non_shared_ha_deployment_obj = get_object_value(ha_deployment_obj, 'nonSharedHaDeployment')
            if non_shared_ha_deployment_obj:
                deployment_type = 'non_shared_ha'
            vm1_name = separator.join([deployment_name, 'vm'+str(currentNodeCount)])

        # common vm dependencies
        dependencies = []
        metadata = {'dependsOn': dependencies}

        # sole-tentant node setup for vm1
        sole_tenant_node_obj = get_sole_tenant_node_obj(context)
        if sole_tenant_node_obj:
            sole_tenant_node_group_name = get_object_value(sole_tenant_node_obj, 'vm1NodeGroup')
            sole_tenant_node_type = get_object_value(sole_tenant_node_obj, 'nodeType')

            # create NodeTemplate and NodeGroups (1 for SN, 2 for HA), if node group names were not given in the configuration
            if not sole_tenant_node_group_name:
                sole_tenant_node_group_name = separator.join([vm1_name,node_group])
                sole_tenant_node_template_name = separator.join([deployment_name+resourcePrefix, node_template])
                # Create Node Template for both VMs
                resources.append(build_sole_tenancy_node_template(context, sole_tenant_node_template_name, sole_tenant_node_group_name, compute_node_template_v1, sole_tenant_node_type))
                # Create Node group for VM1
                resources.append(build_sole_tenancy_node_group(context, sole_tenant_node_group_name, sole_tenant_node_template_name, compute_node_group_v1))
                # add dependency for this resource
                dependencies.append(sole_tenant_node_group_name)

            sole_tenancy_node_affinities = [
                    {
                        'key': 'compute.googleapis.com/node-group-name',
                        'operator': 'IN',
                        'values': [
                            sole_tenant_node_group_name
                        ]
                    }
                ]

        vm1_node_mgmt_ip_name = separator.join([vm1_name, ip, nic, node_mgmt_interface])
        cluster_management_ip_name = separator.join([vm1_name, ip, nic, cluster_mgmt_interface])

        #check if data_nic data object is requested
        data_nic_obj = get_data_nic_obj(context)
        if data_nic_obj:
            data_nic_vpc_project = get_object_value(data_nic_obj, 'project')
            if not data_nic_vpc_project:
                data_nic_vpc_project = project_name

        vm1_iscsi_ip_name = separator.join([vm1_name, ip, nic, iscsi_interface])
        vm1_nas_ip_name = separator.join([vm1_name, ip, nic, nas_interface])

        if deployment_type == 'singlenode':
            if data_nic_obj:
                data_nic = 'nic1'
                vm1_intercluster_ip_name = separator.join([vm1_name, ip, data_nic, intercluster_interface])
                vm1_iscsi_ip_name = separator.join([vm1_name, ip, data_nic, iscsi_interface])
                vm1_nas_ip_name = separator.join([vm1_name, ip, data_nic, nas_interface])

            # create ip address resources
            addresses = {
                'vm1_node_mgmt_ip_name': vm1_node_mgmt_ip_name,
                'cluster_management_ip_name': cluster_management_ip_name,
                'vm1_iscsi_ip_name': vm1_iscsi_ip_name,
                'vm1_nas_ip_name': vm1_nas_ip_name
            }

            if data_nic_obj:
                addresses['vm1_intercluster_ip_name'] = vm1_intercluster_ip_name

            for resource_name in addresses.values():
                if 'nic0' in resource_name:
                    subnet_project = vnic0_vpc_project
                    subnet = vnic0_obj['subnet']
                elif 'nic1' in resource_name:
                    subnet_project = data_nic_vpc_project
                    subnet = data_nic_obj['subnet']

                resources.append(
                    build_address_resource(context, resource_name, compute_address_v1, description, gce_endpoint, subnet_project, subnet)
                )

            # create disk resources
            vm1_disk_names = [None] * system_disks_count

            resource_name = vm1_disk_names[0] = separator.join([vm1_name, 'disk', 'boot'])
            source_image = 'projects/' + source_image_project + '/global/images/' + source_image_context['name']
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, boot_disk_size_gb, vm1_zone, False, source_image))

            resource_name = vm1_disk_names[1] = separator.join([vm1_name, 'disk', 'nvram'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, nvram_disk_size_gb, vm1_zone))

            resource_name = vm1_disk_names[2] = separator.join([vm1_name, 'disk', 'core'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_balanced, core_disk_size_gb, vm1_zone))

            resource_name = vm1_disk_names[3] = separator.join([vm1_name, 'disk', 'root'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, root_disk_size_gb, vm1_zone))

            resource_name = vm1_disk_names[4] = separator.join([vm1_name, 'disk', 'data1'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

            resource_name = vm1_disk_names[5] = separator.join([vm1_name, 'disk', 'data2'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

            resource_name = vm1_disk_names[6] = separator.join([vm1_name, 'disk', 'data3'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

            resource_name = vm1_disk_names[7] = separator.join([vm1_name, 'disk', 'data4'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

            # create instance resources
            vm1_customdata = 'deployment_type=singlenode\nontap_cloud_platform_serial_number=' + context.properties['platformSerialNumberNode1']

            if nvlog_media_type:
                vm1_customdata = vm1_customdata + '\nnvlog_media_type='+ nvlog_media_type

            # vm1 disks
            vm1_disks = list()
            for i in range(system_disks_count):
                vm1_disks.append(get_pd_resource(vm1_disk_names[i], i==0, scsi))
            # add local-ssd resources (if asked)
            vm1_disks.extend(build_local_ssd_resources(context, vm1_name, vm1_zone, local_ssd_count, local_ssd_interface))

            # metadata
            vm1_metadata_entries = list(common_metadata_entries)
            vm1_metadata_entries.append({
                'key': 'customData',
                'value': vm1_customdata
            })

            # alias ip ranges
            nic0_alias_ip_ranges = list()
            nic0_alias_ip_ranges.append({
                'ipCidrRange': '$(ref.' + cluster_management_ip_name + '.address)'
            })

            if not data_nic_obj:
                nic0_alias_ip_ranges.append({
                    'ipCidrRange': '$(ref.' + vm1_iscsi_ip_name + '.address)'
                })
                nic0_alias_ip_ranges.append({
                    'ipCidrRange': '$(ref.' + vm1_nas_ip_name + '.address)'
                })

            resource_name = vm1_name
            vm1_resource = {
                'name': resource_name,
                'type': compute_instance_v1,
                'properties': {
                    'name': resource_name,
                    'labels': labels,
                    'serviceAccounts': service_accounts,
                    'machineType': 'zones/' + vm1_zone + '/machineTypes/' + context.properties['machineType'],
                    'zone': vm1_zone,
                    'metadata': {
                        'items': vm1_metadata_entries
                    },
                    'networkPerformanceConfig' : {
                        'totalEgressBandwidthTier': nic_tier
                    },
                    'networkInterfaces': [
                        {
                            'subnetwork': 'projects/' + vnic0_vpc_project + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic0_obj['subnet'],
                            'networkIP': '$(ref.' + vm1_node_mgmt_ip_name + '.address)',
                            'nicType': nic_type,
                            'aliasIpRanges': nic0_alias_ip_ranges,
                            'accessConfigs':
                            [
                                {
                                    'name': 'External NAT',
                                    'type': 'ONE_TO_ONE_NAT'
                                }
                            ]
                        }
                    ],
                    'disks': vm1_disks,
                    'scheduling': {
                        'preemptible': is_preemptible
                    }
                }
            }

            if sole_tenant_node_obj:
                vm1_resource['properties']['scheduling']['nodeAffinities'] = sole_tenancy_node_affinities

            if data_nic_obj:
                vm1_data_nic = {
                            'subnetwork': 'projects/' + data_nic_vpc_project + '/regions/' + context.properties['region'] + '/subnetworks/' + data_nic_obj['subnet'],
                            'networkIP': '$(ref.' + vm1_intercluster_ip_name + '.address)',
                            'nicType': nic_type,
                            'aliasIpRanges': [
                                {
                                    'ipCidrRange': '$(ref.' + vm1_iscsi_ip_name + '.address)'
                                },
                                {
                                    'ipCidrRange': '$(ref.' + vm1_nas_ip_name + '.address)'
                                }
                            ]
                }
                vm1_resource['properties']['networkInterfaces'].append(vm1_data_nic)

            # Add minCpuPlatform if provided by user
            if min_cpu_platform:
                vm1_resource['properties']['minCpuPlatform'] = min_cpu_platform

            # add dependencies
            vm1_resource['metadata'] = metadata

            resources.append(vm1_resource)

        else: # ha deployment
            # constants
            nic1 = 'nic1'
            nic2 = 'nic2'
            nic3 = 'nic3'
            data_nic = 'nic4'
            cluster_interface = 'cluster'
            interconnect_interface = 'interconnect'
            rsm_interface = 'rsm'

            instance_group_names = {}
            ilb_names = {}

            health_check_names = {}
            tcp_backend_service_names = {}
            udp_backend_service_names = {}
            tcp_forwarding_rule_names = {}
            udp_forwarding_rule_names = {}

            vm2_name = separator.join([deployment_name, 'vm'+str(currentPartnerNodeCount)])
            # extract/set zone for vm2 resources
            vm2_zone = get_object_value(ha_deployment_obj, 'zone')
            if not vm2_zone:
                vm2_zone = vm1_zone
            else:
                # extract zone
                if vm2_zone.rpartition("-")[0] != vm1_region:
                    error_msg = "Supplied zone [%s] for VM2 does not belong to supplied region [%s]." % (vm2_zone, vm1_region)
                    sys.exit(error_msg)

            vm1_cluster_ip_name = separator.join([vm1_name, ip, nic1, cluster_interface])
            vm1_interconnect_ip_name = separator.join([vm1_name, ip, nic2, interconnect_interface])
            vm1_rsm_ip_name = separator.join([vm1_name, ip, nic3, rsm_interface])
            vm2_node_mgmt_ip_name = separator.join([vm2_name, ip, nic, node_mgmt_interface])
            vm2_cluster_ip_name = separator.join([vm2_name, ip, nic1, cluster_interface])
            vm2_interconnect_ip_name = separator.join([vm2_name, ip, nic2, interconnect_interface])
            vm2_rsm_ip_name = separator.join([vm2_name, ip, nic3, rsm_interface])
            if data_nic_obj:
                vm1_iscsi_ip_name = separator.join([vm1_name, ip, data_nic, iscsi_interface])
                vm1_intercluster_ip_name = separator.join([vm1_name, ip, data_nic, intercluster_interface])
                vm2_iscsi_ip_name = separator.join([vm2_name, ip, data_nic, iscsi_interface])
                vm2_intercluster_ip_name = separator.join([vm2_name, ip, data_nic, intercluster_interface])
            else:
                vm2_iscsi_ip_name = separator.join([vm2_name, ip, nic, iscsi_interface])

            vm1_nas_interface = separator.join([nas_interface, 'vm'+str(currentNodeCount)])
            vm2_nas_interface = separator.join([nas_interface, 'vm'+str(currentPartnerNodeCount)])

            if not isBlockDeployment:
                ### creating ilb names
                ilb_counter = 1
                ilb_names['cluster_mgmt_interface'] = separator.join([deployment_name+resourcePrefix, 'ilb'+str(ilb_counter), cluster_mgmt_interface])
                ilb_counter+=1
                ilb_names['vm1_nas_interface'] = separator.join([deployment_name, 'ilb'+str(ilb_counter), vm1_nas_interface])
                ilb_counter+=1
                ilb_names['vm2_nas_interface'] = separator.join([deployment_name, 'ilb'+str(ilb_counter), vm2_nas_interface])

                # creating ip names
                cluster_management_ip_name = separator.join([ilb_names['cluster_mgmt_interface'], ip])
                vm1_nas_ip_name = separator.join([ilb_names['vm1_nas_interface'], ip])
                vm2_nas_ip_name = separator.join([ilb_names['vm2_nas_interface'], ip])

            vnic1_obj = ha_deployment_obj['virtualNetworkInterface1']
            vnic1_vpc_project = get_object_value(vnic1_obj, 'project')
            if not vnic1_vpc_project:
                vnic1_vpc_project = project_name

            vnic2_obj = ha_deployment_obj['virtualNetworkInterface2']
            vnic2_vpc_project = get_object_value(vnic2_obj, 'project')
            if not vnic2_vpc_project:
                vnic2_vpc_project = project_name

            if deployment_type == 'non_shared_ha':
                vnic3_obj = non_shared_ha_deployment_obj['virtualNetworkInterface3']
                vnic3_vpc_project = get_object_value(vnic3_obj, 'project')
                if not vnic3_vpc_project:
                    vnic3_vpc_project = project_name

                mediator_obj = non_shared_ha_deployment_obj['mediator']
                mediator_ip = get_object_value(mediator_obj, 'mediatorIp')
                mediator_disks_count = 2
                if mediator_ip:
                    create_mediator_resources = False
                else:
                    create_mediator_resources = True
                    mediator_name = separator.join([deployment_name+resourcePrefix, 'mediator'])
                    mediator_ip_name = separator.join([mediator_name, ip, nic])
                    mediator_metadata_entries = list(common_metadata_entries)

                    # retrieve details about Mediator image
                    mediator_image_obj = mediator_obj['mediatorImage']
                    mediator_image_project = get_object_value(mediator_image_obj, 'project')
                    if not mediator_image_project:
                        mediator_image_project = project_name

                    # retrieve details about Mediator Primary NIC
                    mediator_vnic0_obj = mediator_obj['virtualNetworkInterface0']
                    mediator_vnic0_vpc_project = get_object_value(mediator_vnic0_obj, 'project')
                    if not mediator_vnic0_vpc_project:
                        mediator_vnic0_vpc_project = project_name
                    mediator_vnic0_subnet = get_object_value(mediator_vnic0_obj, 'subnet')
                    if not mediator_vnic0_subnet:
                        mediator_vnic0_subnet = vnic3_obj['subnet']
                    # extract/set zone for vm2 resources
                    mediator_machine_type = mediator_obj['mediatorMachineType']
                    mediator_zone = get_object_value(mediator_obj, 'zone')
                    if not mediator_zone:
                        mediator_zone = vm2_zone
                    else:
                        # extract zone
                        if mediator_zone.rpartition("-")[0] != vm1_region:
                            error_msg = "Supplied zone [%s] for Mediator does not belong to supplied region [%s]." % (mediator_zone, vm1_region)
                            sys.exit(error_msg)

            # determine the type of HA deployment
            vms_in_placement_policy = set()
            if vm1_zone == vm2_zone:
                ha_deployment_type = 'single-az'
                vms_in_placement_policy = {vm1_name, vm2_name}
                if deployment_type == 'non_shared_ha':
                    if create_mediator_resources == True and vm1_zone == mediator_zone:
                        vms_in_placement_policy = {vm1_name, vm2_name, mediator_name}
            else:
                ha_deployment_type = 'cross-az'
                if deployment_type == 'non_shared_ha':
                    if create_mediator_resources == True:
                        if vm1_zone == mediator_zone:
                            vms_in_placement_policy = {vm1_name, mediator_name}
                        if vm2_zone == mediator_zone:
                            vms_in_placement_policy = {vm2_name, mediator_name}
            # total vms participating in placement policy (if any)
            placement_policy_vm_count = len(vms_in_placement_policy)

            # create ip address resources for nics
            addresses = {
                'vm1_node_mgmt_ip_name': vm1_node_mgmt_ip_name,
                'vm1_iscsi_ip_name': vm1_iscsi_ip_name,
                'vm1_cluster_ip_name': vm1_cluster_ip_name,
                'vm1_interconnect_ip_name': vm1_interconnect_ip_name,
                'vm2_node_mgmt_ip_name': vm2_node_mgmt_ip_name,
                'vm2_iscsi_ip_name': vm2_iscsi_ip_name,
                'vm2_cluster_ip_name': vm2_cluster_ip_name,
                'vm2_interconnect_ip_name': vm2_interconnect_ip_name
            }

            if deployment_type == 'non_shared_ha':
                addresses['vm1_rsm_ip_name'] = vm1_rsm_ip_name
                addresses['vm2_rsm_ip_name'] = vm2_rsm_ip_name
                if create_mediator_resources:
                    addresses['mediator_ip_name'] = mediator_ip_name

            if data_nic_obj:
                addresses['vm1_intercluster_ip_name'] = vm1_intercluster_ip_name
                addresses['vm2_intercluster_ip_name'] = vm2_intercluster_ip_name
                vnic_vpc_project_for_resources  = data_nic_vpc_project
                vnic_subnet_obj_for_resources = data_nic_obj['subnet']
                vnic_network_obj_for_resources = data_nic_obj['network']
            else:
                vnic_vpc_project_for_resources  = vnic0_vpc_project
                vnic_subnet_obj_for_resources = vnic0_obj['subnet']
                vnic_network_obj_for_resources = vnic0_obj['network']

            for resource_name in addresses.values():
                ip_address_to_assign = None
                if 'mediator' in resource_name:
                    subnet_project = mediator_vnic0_vpc_project
                    subnet = mediator_vnic0_subnet
                elif 'nic0' in resource_name:
                    subnet_project = vnic0_vpc_project
                    subnet = vnic0_obj['subnet']
                else:
                    subnet_project = project_name
                    if 'nic1' in resource_name:
                        subnet_project = vnic1_vpc_project
                        subnet = vnic1_obj['subnet']
                    elif 'nic2' in resource_name:
                        subnet_project = vnic2_vpc_project
                        subnet = vnic2_obj['subnet']
                    elif 'nic3' in resource_name:
                        subnet_project = vnic3_vpc_project
                        subnet = vnic3_obj['subnet']
                    elif 'nic4' in resource_name:
                        if intercluster_lif_ips is not None and vm1_name+'-ip-nic4-intercluster' in resource_name:
                            ip_address_to_assign = intercluster_lif_ips[currentNodeCount-1]
                        elif intercluster_lif_ips is not None and vm2_name+'-ip-nic4-intercluster' in resource_name:
                            ip_address_to_assign = intercluster_lif_ips[currentPartnerNodeCount-1]
                        elif iscsi_lif_ips is not None and vm1_name+'-ip-nic4-iscsi' in resource_name:
                            ip_address_to_assign = iscsi_lif_ips[currentNodeCount-1]
                        elif iscsi_lif_ips is not None and vm2_name+'-ip-nic4-iscsi' in resource_name:
                            ip_address_to_assign = iscsi_lif_ips[currentPartnerNodeCount-1]

                        subnet_project = vnic_vpc_project_for_resources
                        subnet = vnic_subnet_obj_for_resources
                    else:
                        error_msg = "Invalid address name found: [%s]." % (resource_name)
                        sys.exit(error_msg)

                resources.append(
                    build_address_resource(context, resource_name, compute_address_v1, description, gce_endpoint, subnet_project, subnet, ip_address_to_assign)
                )

            resources.append(build_address_resource(context, cluster_management_ip_name, compute_address_v1, description, shared_loadbalancer_vip, vnic0_vpc_project, vnic0_obj['subnet']))

            if not isBlockDeployment:
                addresses = {
                    'vm1_nas_ip_name': vm1_nas_ip_name,
                    'vm2_nas_ip_name': vm2_nas_ip_name
                }
                for resource_name in addresses.values():
                    resources.append(
                        build_address_resource(context, resource_name, compute_address_v1, description, shared_loadbalancer_vip, vnic_vpc_project_for_resources, vnic_subnet_obj_for_resources)
                    )

            # create instance group resources
            resource_name = instance_group_names['instancegroup1'] = separator.join([deployment_name+resourcePrefix, "instancegroup1"])
            resources.append(build_instance_group_resource(resource_name, compute_instance_group_v1, description, vm1_zone))

            resource_name = instance_group_names['instancegroup2'] = separator.join([deployment_name+resourcePrefix, "instancegroup2"])
            resources.append(build_instance_group_resource(resource_name, compute_instance_group_v1, description, vm2_zone))


            # create health check resources
            if not isBlockDeployment:
                resource_name = health_check_names['cluster_mgmt_interface'] = separator.join([ilb_names['cluster_mgmt_interface'], "healthcheck"])
                resources.append(build_health_check_resource(context, resource_name, compute_health_check_v1, description, 63001))

                resource_name = health_check_names['vm1_nas_interface'] = separator.join([ilb_names['vm1_nas_interface'], "healthcheck"])
                resources.append(build_health_check_resource(context, resource_name, compute_health_check_v1, description, 63002))

                resource_name = health_check_names['vm2_nas_interface'] = separator.join([ilb_names['vm2_nas_interface'], "healthcheck"])
                resources.append(build_health_check_resource(context, resource_name, compute_health_check_v1, description, 63003))


                # create backend services
                resource_name = tcp_backend_service_names['cluster_mgmt_interface'] = separator.join([ilb_names['cluster_mgmt_interface'], "backendservice-tcp"])
                resources.append(
                    build_backend_service_resource(context, resource_name, compute_backend_service_v1, description, vnic0_vpc_project, vnic0_obj['network'],
                                                tcp, instance_group_names['instancegroup1'], instance_group_names['instancegroup2'], health_check_names['cluster_mgmt_interface'])
                )
                resource_name = udp_backend_service_names['cluster_mgmt_interface'] = separator.join([ilb_names['cluster_mgmt_interface'], "backendservice-udp"])
                resources.append(
                    build_backend_service_resource(context, resource_name, compute_backend_service_v1, description, vnic0_vpc_project, vnic0_obj['network'],
                                                udp, instance_group_names['instancegroup1'], instance_group_names['instancegroup2'], health_check_names['cluster_mgmt_interface'])
                )

                resource_name = tcp_backend_service_names['vm1_nas_interface'] = separator.join([ilb_names['vm1_nas_interface'], "backendservice-tcp"])

                resources.append(
                    build_backend_service_resource(context, resource_name, compute_backend_service_v1, description, vnic_vpc_project_for_resources, vnic_network_obj_for_resources,
                                                tcp, instance_group_names['instancegroup1'], instance_group_names['instancegroup2'], health_check_names['vm1_nas_interface'])
                )
                resource_name = udp_backend_service_names['vm1_nas_interface'] = separator.join([ilb_names['vm1_nas_interface'], "backendservice-udp"])
                resources.append(
                    build_backend_service_resource(context, resource_name, compute_backend_service_v1, description, vnic_vpc_project_for_resources, vnic_network_obj_for_resources,
                                                udp, instance_group_names['instancegroup1'], instance_group_names['instancegroup2'], health_check_names['vm1_nas_interface'])
                )

                resource_name = tcp_backend_service_names['vm2_nas_interface'] = separator.join([ilb_names['vm2_nas_interface'], "backendservice-tcp"])
                resources.append(
                    build_backend_service_resource(context, resource_name, compute_backend_service_v1, description, vnic_vpc_project_for_resources, vnic_network_obj_for_resources,
                                                tcp, instance_group_names['instancegroup1'], instance_group_names['instancegroup2'], health_check_names['vm2_nas_interface'])
                )
                resource_name = udp_backend_service_names['vm2_nas_interface'] = separator.join([ilb_names['vm2_nas_interface'], "backendservice-udp"])
                resources.append(
                    build_backend_service_resource(context, resource_name, compute_backend_service_v1, description, vnic_vpc_project_for_resources, vnic_network_obj_for_resources,
                                                udp, instance_group_names['instancegroup1'], instance_group_names['instancegroup2'], health_check_names['vm2_nas_interface'])
                )


                # create forwarding rule resources
                resource_name = tcp_forwarding_rule_names['cluster_mgmt_interface'] = separator.join([ilb_names['cluster_mgmt_interface'], "forwardingrule-tcp"])
                resources.append(
                    build_forwarding_rule_resource(context, resource_name, compute_forwarding_rule_v1, description, vnic0_vpc_project, vnic0_obj['network'],
                                                vnic0_obj['subnet'], cluster_management_ip_name, tcp, tcp_backend_service_names['cluster_mgmt_interface'], True)
                )
                resource_name = udp_forwarding_rule_names['cluster_mgmt_interface'] = separator.join([ilb_names['cluster_mgmt_interface'], "forwardingrule-udp"])
                resources.append(
                    build_forwarding_rule_resource(context, resource_name, compute_forwarding_rule_v1, description, vnic0_vpc_project, vnic0_obj['network'],
                                                vnic0_obj['subnet'], cluster_management_ip_name, udp, udp_backend_service_names['cluster_mgmt_interface'], True)
                )

                # assign static ilb ips if provided
                ip_to_assign = None
                if ilb_ips is not None:
                    ip_to_assign = ilb_ips[currentHaIndex-1]

                resource_name = tcp_forwarding_rule_names['vm1_nas_interface'] = separator.join([ilb_names['vm1_nas_interface'], "forwardingrule-tcp"])
                resources.append(
                    build_forwarding_rule_resource(context, resource_name, compute_forwarding_rule_v1, description, vnic_vpc_project_for_resources, vnic_network_obj_for_resources,
                                                vnic_subnet_obj_for_resources, vm1_nas_ip_name, tcp, tcp_backend_service_names['vm1_nas_interface'], False, ip_to_assign)
                )
                resource_name = udp_forwarding_rule_names['vm1_nas_interface'] = separator.join([ilb_names['vm1_nas_interface'], "forwardingrule-udp"])
                resources.append(
                    build_forwarding_rule_resource(context, resource_name, compute_forwarding_rule_v1, description, vnic_vpc_project_for_resources, vnic_network_obj_for_resources,
                                                vnic_subnet_obj_for_resources, vm1_nas_ip_name, udp, udp_backend_service_names['vm1_nas_interface'])
                )

                resource_name = tcp_forwarding_rule_names['vm2_nas_interface'] = separator.join([ilb_names['vm2_nas_interface'], "forwardingrule-tcp"])
                resources.append(
                    build_forwarding_rule_resource(context, resource_name, compute_forwarding_rule_v1, description, vnic_vpc_project_for_resources, vnic_network_obj_for_resources,
                                                vnic_subnet_obj_for_resources, vm2_nas_ip_name, tcp, tcp_backend_service_names['vm2_nas_interface'])
                )
                resource_name = udp_forwarding_rule_names['vm2_nas_interface'] = separator.join([ilb_names['vm2_nas_interface'], "forwardingrule-udp"])
                resources.append(
                    build_forwarding_rule_resource(context, resource_name, compute_forwarding_rule_v1, description, vnic_vpc_project_for_resources, vnic_network_obj_for_resources,
                                                vnic_subnet_obj_for_resources, vm2_nas_ip_name, udp, udp_backend_service_names['vm2_nas_interface'])
                )

            # create disk resources
            vm1_disk_names = [None] * system_disks_count
            vm2_disk_names = [None] * system_disks_count

            # first, vm1 disks
            resource_name = vm1_disk_names[0] = separator.join([vm1_name, 'disk', 'boot'])
            source_image = 'projects/' + source_image_project + '/global/images/' + source_image_context['name']
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, boot_disk_size_gb, vm1_zone, False, source_image))

            resource_name = vm1_disk_names[1] = separator.join([vm1_name, 'disk', 'nvram'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, nvram_disk_size_gb, vm1_zone))

            resource_name = vm1_disk_names[2] = separator.join([vm1_name, 'disk', 'core'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_balanced, core_disk_size_gb, vm1_zone))

            resource_name = vm1_disk_names[3] = separator.join([vm1_name, 'disk', 'root'])

            if deployment_type == 'shared_ha':
                vm2_disk_names[4] = vm1_disk_names[3]

                if ha_deployment_type == 'single-az':
                    resources.append(
                        build_pd_resource(context, resource_name, compute_disk_beta, labels, pd_ssd, root_disk_size_gb, vm1_zone, True, None)
                    )
                else:
                    # ha_deployment_type is 'cross-az'
                    resources.append(
                        build_pd_resource(context, resource_name, compute_region_disk_alpha, labels, pd_ssd, root_disk_size_gb, vm1_zone, True, None, vm2_zone)
                    )

                resource_name = vm1_disk_names[4] = separator.join([vm1_name, 'disk', 'data1'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

                resource_name = vm1_disk_names[5] = separator.join([vm1_name, 'disk', 'data2'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

                resource_name = vm1_disk_names[6] = separator.join([vm1_name, 'disk', 'data3'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_balanced, data_disk_size_gb, vm1_zone))

                resource_name = vm1_disk_names[7] = separator.join([vm1_name, 'disk', 'data4'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))
            else:
                # deployment_type is 'non_shared_ha'
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, root_disk_size_gb, vm1_zone))

                # create mirror disk for partner root
                resource_name = vm1_disk_names[4] = separator.join([vm2_name, 'disk', 'rootcopy'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, root_disk_size_gb, vm1_zone))

                resource_name = vm1_disk_names[5] = separator.join([vm1_name, 'disk', 'data1'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

                resource_name = vm1_disk_names[6] = separator.join([vm1_name, 'disk', 'data2'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

                resource_name = vm1_disk_names[7] = separator.join([vm1_name, 'disk', 'data3'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

                resource_name = vm1_disk_names[8] = separator.join([vm1_name, 'disk', 'data4'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))



            # now, vm2 disks
            resource_name = vm2_disk_names[0] = separator.join([vm2_name, 'disk', 'boot'])
            source_image = 'projects/' + source_image_project + '/global/images/' + source_image_context['name']
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, boot_disk_size_gb, vm2_zone, False, source_image))

            resource_name = vm2_disk_names[1] = separator.join([vm2_name, 'disk', 'nvram'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, nvram_disk_size_gb, vm2_zone))

            resource_name = vm2_disk_names[2] = separator.join([vm2_name, 'disk', 'core'])
            resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_balanced, core_disk_size_gb, vm2_zone))

            resource_name = vm2_disk_names[3] = separator.join([vm2_name, 'disk', 'root'])

            if deployment_type == 'shared_ha':
                vm1_disk_names[4] = vm2_disk_names[3]

                if ha_deployment_type == 'single-az':
                    resources.append(
                        build_pd_resource(context, resource_name, compute_disk_beta, labels, pd_ssd, root_disk_size_gb, vm2_zone, True, None)
                    )
                else:
                    # ha_deployment_type is 'cross-az'
                    resources.append(
                        build_pd_resource(context, resource_name, compute_region_disk_alpha, labels, pd_ssd, root_disk_size_gb, vm2_zone, True, None, vm1_zone)
                    )

            else:
                # deployment_type is 'non_shared_ha'
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, root_disk_size_gb, vm2_zone))

                # create mirror disk for partner root
                resource_name = vm2_disk_names[4] = separator.join([vm1_name, 'disk', 'rootcopy'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, root_disk_size_gb, vm2_zone))

                resource_name = vm2_disk_names[5] = separator.join([vm1_name, 'disk', 'data1copy'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

                resource_name = vm2_disk_names[6] = separator.join([vm1_name, 'disk', 'data2copy'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

                resource_name = vm2_disk_names[7] = separator.join([vm1_name, 'disk', 'data3copy'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

                resource_name = vm2_disk_names[8] = separator.join([vm1_name, 'disk', 'data4copy'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_ssd, data_disk_size_gb, vm1_zone))

            # create a placement policy
            if placement_policy_vm_count:
                resource_name = placement_policy_name = separator.join([deployment_name+resourcePrefix, "vm", "placementpolicy1", "spread"])
                resources.append({
                    'name': resource_name,
                    'type': compute_resource_policy_v1,
                    'properties': {
                        'name': resource_name,
                        'description': description,
                        'groupPlacementPolicy': {
                            'availabilityDomainCount': placement_policy_vm_count
                        },
                        'region': context.properties['region']
                    }
                })

            if deployment_type == 'non_shared_ha' and create_mediator_resources:
                # create mediator disk resources
                mediator_disk_names = [None] * mediator_disks_count

                resource_name = mediator_disk_names[0] = separator.join([mediator_name, 'disk', 'boot'])
                source_image = 'projects/' + mediator_image_project + '/global/images/' + mediator_image_obj['name']
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_balanced, boot_disk_size_gb, mediator_zone, False, source_image))

                resource_name = mediator_disk_names[1] = separator.join([mediator_name, 'disk', 'data'])
                resources.append(build_pd_resource(context, resource_name, compute_disk_v1, labels, pd_balanced, boot_disk_size_gb, mediator_zone))

                mediator_disks = list()
                for i in range(mediator_disks_count):
                    mediator_disks.append(get_pd_resource(mediator_disk_names[i], i==0, scsi))

                # create mediator vm resource
                mediator_metadata_entries = list(common_metadata_entries)
                mediator_metadata_entries.append({
                        'key': 'iscsi-mediator-target',
                        'value': mediator_obj['mediatorTarget']+ str(currentHaIndex)
                    })

                resource_name = mediator_name
                mediator_resource = {
                    'name': resource_name,
                    'type': compute_instance_v1,
                    'properties': {
                        'name': resource_name,
                        'labels': labels,
                        'serviceAccounts': service_accounts,
                        'machineType': 'zones/' + mediator_zone + '/machineTypes/' + mediator_machine_type,
                        'zone': mediator_zone,
                        'metadata': {
                            'items': mediator_metadata_entries
                        },
                        'networkInterfaces': [
                            {
                                'subnetwork': 'projects/' + mediator_vnic0_vpc_project + '/regions/' + context.properties['region'] + '/subnetworks/' + mediator_vnic0_subnet,
                                'networkIP': '$(ref.' + mediator_ip_name + '.address)'
                            }
                        ],
                        'disks': mediator_disks,
                        'scheduling': {
                            'preemptible': is_preemptible
                        }
                    }
                }
                # add to resource policy if any
                if mediator_name in vms_in_placement_policy:
                    mediator_resource['properties']['resourcePolicies'] = ['$(ref.' + placement_policy_name + '.selfLink)']

                resources.append(mediator_resource)

            # create instance resources
            # add common vm dependencies
            for index in tcp_forwarding_rule_names.keys():
                dependencies.append(tcp_forwarding_rule_names[index])
            for index in udp_forwarding_rule_names.keys():
                dependencies.append(udp_forwarding_rule_names[index])

            if deployment_type == 'non_shared_ha' and create_mediator_resources:
                dependencies.append(mediator_name)

            # common customdata
            common_customdata = 'deployment_type=' + deployment_type + '\ncvo_self_setup=true' + '\ndeployment_name=' + deployment_name

            if nvlog_media_type:
                common_customdata = common_customdata + '\nnvlog_media_type='+ nvlog_media_type

            if deployment_type == 'non_shared_ha':
                if create_mediator_resources:
                    mediator_ip_address = '$(ref.' + mediator_ip_name + '.address)'
                else:
                    mediator_ip_address = mediator_ip
                common_customdata = common_customdata + '\niscsi_mediator_ip=' + mediator_ip_address + '\niscsi_mediator_target=' + mediator_obj['mediatorTarget'] + str(currentHaIndex)

            # this ip will be used to join nodes to the cluster (mainly in the case of >1 ha pair)
            if currentNodeCount == 1:
                cluster_join_ip = '$(ref.' + vm1_cluster_ip_name + '.address)'

            # first, vm1
            vm1_customdata = common_customdata + '\nontap_cloud_platform_serial_number=' + context.properties['platformSerialNumberNode1'] + '\nic_partner_ip=' + '$(ref.' + vm2_interconnect_ip_name + '.address)' + '\ncluster_mgmt_ip=' + '$(ref.' + cluster_management_ip_name + '.address)'
            if deployment_type == 'non_shared_ha':
                vm1_customdata = vm1_customdata + '\niscsi_partner_ip=' + '$(ref.' + vm2_rsm_ip_name + '.address)'

            vm1_customdata = vm1_customdata + '\nsubnet_address_prefix=na\ncluster_local_ip=' + '$(ref.' + vm1_cluster_ip_name + '.address)' + '\ncluster_partner_ip=' + '$(ref.' + vm2_cluster_ip_name + '.address)' + '\nmgmt_local_ip=' + '$(ref.' + vm1_node_mgmt_ip_name + '.address)' + '\nmgmt_partner_ip=' + '$(ref.' + vm2_node_mgmt_ip_name + '.address)' + '\nvm_name=' + vm1_name + '\nvm_partner_name=' + vm2_name + '\nis_cluster_joining_node='+ is_cluster_joining_node + '\ncluster_join_ip=' + cluster_join_ip + '\nnode_index=' + str(currentNodeCount)

            if data_nic_obj:
                vm1_customdata = vm1_customdata + '\nintercluster_local_ip=' + '$(ref.' + vm1_intercluster_ip_name + '.address)' + '\ndata_iscsi_local_ip=' + '$(ref.' + vm1_iscsi_ip_name + '.address)'  + '\ndata_iscsi_partner_ip=' + '$(ref.' + vm2_iscsi_ip_name + '.address)'

            # vm1 disks
            vm1_disks = list()
            for i in range(system_disks_count):
                vm1_disks.append(get_pd_resource(vm1_disk_names[i], i==0, scsi))
            # add local-ssd resources (if asked)
            vm1_disks.extend(build_local_ssd_resources(context, vm1_name, vm1_zone, local_ssd_count, local_ssd_interface))

            # metadata
            vm1_metadata_entries = list(common_metadata_entries)
            vm1_metadata_entries.append({
                'key': 'customData',
                'value': vm1_customdata
            })

            # vm1 iscsi alias ip ranges
            vm1_iscsi_alias_ip_range = list()
            vm1_iscsi_alias_ip_range.append({
                    'ipCidrRange': '$(ref.' + vm1_iscsi_ip_name + '.address)'
            })

            resource_name = vm1_name
            vm1_resource = {
                'name': resource_name,
                'type': compute_instance_v1,
                'properties': {
                    'name': resource_name,
                    'labels': labels,
                    'serviceAccounts': service_accounts,
                    'machineType': 'zones/' + vm1_zone + '/machineTypes/' + context.properties['machineType'],
                    'zone': vm1_zone,
                    'metadata': {
                        'items': vm1_metadata_entries
                    },
                    'networkPerformanceConfig' : {
                        'totalEgressBandwidthTier': nic_tier
                    },
                    'networkInterfaces': [
                        {
                            'subnetwork': 'projects/' + vnic0_vpc_project + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic0_obj['subnet'],
                            'nicType': nic_type,
                            'networkIP': '$(ref.' + vm1_node_mgmt_ip_name + '.address)',
                            'accessConfigs':
                             [
                                {
                                    'name': 'External NAT',
                                    'type': 'ONE_TO_ONE_NAT'
                                }
                             ]
                        },
                        {
                            'subnetwork': 'projects/' + project_name + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic1_obj['subnet'],
                            'nicType': nic_type,
                            'networkIP': '$(ref.' + vm1_cluster_ip_name + '.address)'
                        },
                        {
                            'subnetwork': 'projects/' + project_name + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic2_obj['subnet'],
                            'nicType': nic_type,
                            'networkIP': '$(ref.' + vm1_interconnect_ip_name + '.address)'
                        }
                    ],
                    'disks': vm1_disks,
                    'scheduling': {
                        'preemptible': is_preemptible
                    }
                }
            }

            if sole_tenant_node_obj:
                vm1_resource['properties']['scheduling']['nodeAffinities'] = sole_tenancy_node_affinities

            # add minCpuPlatform if provided by user
            if min_cpu_platform:
                vm1_resource['properties']['minCpuPlatform'] = min_cpu_platform
            if deployment_type == 'non_shared_ha':
                vm1_rsm_nic = {
                    'subnetwork': 'projects/' + project_name + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic3_obj['subnet'],
                    'nicType': nic_type,
                    'networkIP': '$(ref.' + vm1_rsm_ip_name + '.address)'
                }
                vm1_resource['properties']['networkInterfaces'].append(vm1_rsm_nic)

            if data_nic_obj:
                vm1_data_nic = {
                    'subnetwork': 'projects/' + project_name   + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic_subnet_obj_for_resources,
                    'networkIP': '$(ref.' + vm1_intercluster_ip_name + '.address)',
                    'nicType': nic_type,
                    'aliasIpRanges': vm1_iscsi_alias_ip_range
                }
                vm1_resource['properties']['networkInterfaces'].append(vm1_data_nic)
            else:
                vm1_resource['properties']['networkInterfaces'][0]['aliasIpRanges'] = vm1_iscsi_alias_ip_range

            # add to resource policy if any
            if vm1_name in vms_in_placement_policy:
                vm1_resource['properties']['resourcePolicies'] = ['$(ref.' + placement_policy_name + '.selfLink)']

            # add dependencies
            vm1_resource['metadata'] = metadata

            resources.append(vm1_resource)

            # now, vm2
            vm2_customdata = common_customdata + '\nontap_cloud_platform_serial_number=' + ha_deployment_obj['platformSerialNumberNode2'] + '\nic_partner_ip=' + '$(ref.' + vm1_interconnect_ip_name + '.address)' + '\npartner_cluster_ip=' + '$(ref.' + vm1_cluster_ip_name + '.address)'
            vm2_customdata = vm2_customdata + '\nsubnet_address_prefix=na\ncluster_local_ip=' + '$(ref.' + vm2_cluster_ip_name + '.address)' + '\ncluster_partner_ip=' + '$(ref.' + vm1_cluster_ip_name + '.address)' + '\nmgmt_local_ip=' + '$(ref.' + vm2_node_mgmt_ip_name + '.address)' + '\nmgmt_partner_ip=' + '$(ref.' + vm1_node_mgmt_ip_name + '.address)' + '\ncluster_mgmt_ip=' + '$(ref.' + cluster_management_ip_name + '.address)' + '\nvm_name=' + vm2_name + '\nvm_partner_name='+vm1_name+ '\nis_cluster_joining_node=true' + '\ncluster_join_ip=' + cluster_join_ip + '\nnode_index=' + str(currentPartnerNodeCount)

            if data_nic_obj:
                vm2_customdata = vm2_customdata + '\nintercluster_local_ip=' + '$(ref.' + vm2_intercluster_ip_name + '.address)' + '\ndata_iscsi_local_ip=' + '$(ref.' + vm2_iscsi_ip_name + '.address)' + '\ndata_iscsi_partner_ip=' + '$(ref.' + vm1_iscsi_ip_name + '.address)'
            if deployment_type == 'non_shared_ha':
                vm2_customdata = vm2_customdata + '\niscsi_partner_ip=' + '$(ref.' + vm1_rsm_ip_name + '.address)'

            vm2_disks = list()
            for i in range(system_disks_count):
                vm2_disks.append(get_pd_resource(vm2_disk_names[i], i==0, scsi))
            # add local-ssd resources (if asked)
            vm2_disks.extend(build_local_ssd_resources(context, vm2_name, vm2_zone, local_ssd_count, local_ssd_interface))

            # metadata
            vm2_metadata_entries = list(common_metadata_entries)
            vm2_metadata_entries.append({
                'key': 'customData',
                'value': vm2_customdata
            })

            # vm1 alias ip ranges
            vm2_iscsi_alias_ip_range = list()
            vm2_iscsi_alias_ip_range.append({
                    'ipCidrRange': '$(ref.' + vm2_iscsi_ip_name + '.address)'
            })

            resource_name = vm2_name
            vm2_resource = {
                'name': resource_name,
                'type': compute_instance_v1,
                'properties': {
                    'name': resource_name,
                    'labels': labels,
                    'serviceAccounts': service_accounts,
                    'machineType': 'zones/' + vm2_zone + '/machineTypes/' + context.properties['machineType'],
                    'zone': vm2_zone,
                    'metadata': {
                        'items': vm2_metadata_entries
                    },
                    'networkPerformanceConfig' : {
                        'totalEgressBandwidthTier': nic_tier
                    },
                    'networkInterfaces': [
                        {
                            'subnetwork': 'projects/' + vnic0_vpc_project + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic0_obj['subnet'],
                            'nicType': nic_type,
                            'networkIP': '$(ref.' + vm2_node_mgmt_ip_name + '.address)'                   },
                        {
                            'subnetwork': 'projects/' + project_name + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic1_obj['subnet'],
                            'nicType': nic_type,
                            'networkIP': '$(ref.' + vm2_cluster_ip_name + '.address)'
                        },
                        {
                            'subnetwork': 'projects/' + project_name + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic2_obj['subnet'],
                            'nicType': nic_type,
                            'networkIP': '$(ref.' + vm2_interconnect_ip_name + '.address)'
                        }
                    ],
                    'disks': vm2_disks,
                    'scheduling': {
                        'preemptible': is_preemptible
                    }
                }
            }

            if sole_tenant_node_obj:
                # get nodeGroup name for vm2 from configuration
                sole_tenant_node_group_name = get_object_value(sole_tenant_node_obj, 'vm2NodeGroup')

                # Create Node group for vm2, if vm2NodeGroup is not in configuration
                if not sole_tenant_node_group_name:
                    sole_tenant_node_group_name = separator.join([vm2_name,node_group])
                    resources.append(build_sole_tenancy_node_group(context, sole_tenant_node_group_name, sole_tenant_node_template_name, compute_node_group_v1))
                    dependencies.append(sole_tenant_node_group_name)

                sole_tenancy_node_affinities = [
                {
                        'key': 'compute.googleapis.com/node-group-name',
                        'operator': 'IN',
                        'values': [
                            sole_tenant_node_group_name
                        ]
                    }
                ]
                vm2_resource['properties']['scheduling']['nodeAffinities'] = sole_tenancy_node_affinities

            # Add minCpuPlatform if provided by user
            if min_cpu_platform:
                vm2_resource['properties']['minCpuPlatform'] = min_cpu_platform

            if deployment_type == 'non_shared_ha':
                vm2_rsm_nic = {
                    'subnetwork': 'projects/' + project_name + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic3_obj['subnet'],
                    'nicType': nic_type,
                    'networkIP': '$(ref.' + vm2_rsm_ip_name + '.address)'
                }
                vm2_resource['properties']['networkInterfaces'].append(vm2_rsm_nic)

            if data_nic_obj:
                vm2_data_nic = {
                    'subnetwork': 'projects/' + project_name   + '/regions/' + context.properties['region'] + '/subnetworks/' + vnic_subnet_obj_for_resources,
                    'networkIP': '$(ref.' + vm2_intercluster_ip_name + '.address)',
                    'nicType': nic_type,
                    'aliasIpRanges': vm2_iscsi_alias_ip_range
                }
                vm2_resource['properties']['networkInterfaces'].append(vm2_data_nic)
            else:
                vm2_resource['properties']['networkInterfaces'][0]['aliasIpRanges'] = vm2_iscsi_alias_ip_range

            # add to resource policy if any
            if vm2_name in vms_in_placement_policy:
                vm2_resource['properties']['resourcePolicies'] = ['$(ref.' + placement_policy_name + '.selfLink)']

            # add dependencies
            vm2_resource['metadata'] = metadata

            resources.append(vm2_resource)

            # add these instances to the instance groups
            resource_name = separator.join([instance_group_names['instancegroup1'], 'add', vm1_name])
            resources.append({
                'name': resource_name,
                'action': 'gcp-types/compute-v1:compute.instanceGroups.addInstances',
                'properties': {
                    'instanceGroup': instance_group_names['instancegroup1'],
                    'zone': vm1_zone,
                    'instances': [
                        {
                            'instance': '$(ref.' + vm1_name + '.selfLink)'
                        }
                    ]
                }
            })

            resource_name = separator.join([instance_group_names['instancegroup2'], 'add', vm2_name])
            resources.append({
                'name': resource_name,
                'action': 'gcp-types/compute-v1:compute.instanceGroups.addInstances',
                'properties': {
                    'instanceGroup': instance_group_names['instancegroup2'],
                    'zone': vm2_zone,
                    'instances': [
                        {
                            'instance': '$(ref.' + vm2_name + '.selfLink)'
                        }
                    ]
                }
            })

    return {'resources': resources}