#!/usr/bin/env ruby
# encoding: UTF-8
#
# Copyright © 2016 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require_relative 'types'
require_relative 'services'
require_relative 'parcels'

module CmApi
  module Endpoints
    # Module for Cluster methods and types
    module Clusters
      include ::CmApi::Endpoints::Types

      CLUSTERS_PATH = '/clusters'.freeze

      def create_cluster(name, version = nil, fullVersion = nil)
        if version.nil? && fullVersion.nil?
          raise "Either 'version' or 'fullVersion' must be specified"
        end
        if !fullVersion.nil?
          api_version = 6
          version = nil
        else
          api_version = 1
        end

        apicluster = ApiCluster.new(self, name, version, fullVersion)
        # Ruby port note: as this module is included directly into a resource (ApiResource), we call
        # the :get/:post method directly.  In other BaseApiObject-derived modules (Services/Roles), we must call the
        # method on the @_resource_root instance variable object (ApiResource)
        call_resource(method(:post), CLUSTERS_PATH, ApiCluster, true, [apicluster], nil, api_version)[0]
      end

      def get_cluster(name)
        call_resource(method(:get), "#{CLUSTERS_PATH}/#{name}", ApiCluster)
      end

      def get_all_clusters(view = nil)
        call_resource(method(:get), CLUSTERS_PATH, ApiCluster, true, nil, view && { 'view' => view } || nil)
      end

      def delete_cluster(name)
        call_resource(method(:delete), "#{CLUSTERS_PATH}/#{name}", ApiCluster)
      end

      # Model for a cluster
      class ApiCluster < BaseApiResource
        include ::CmApi::Endpoints::Services
        include ::CmApi::Endpoints::Parcels

        @_ATTRIBUTES = {
          'name' => nil,
          'displayName' => nil,
          'clusterUrl' => nil,
          'version' => nil,
          'fullVersion' => nil,
          'hostsUrl' => ROAttr.new,
          'maintenanceMode' => ROAttr.new,
          'maintenanceOwners' => ROAttr.new,
          'entityStatus' => ROAttr.new
        }

        def initialize(resource_root, name = nil, version = nil, fullVersion = nil)
          # possible alternative to generate the hash argument dynamically, similar to python locals():
          #  method(__method__).parameters.map { |arg| arg[1] }.inject({}) { |h, a| h[a] = eval a.to_s; h}
          super(resource_root, { name: name, version: version, fullVersion: fullVersion })
        end

        def to_s
          "<ApiCluster>: #{@name}; version: #{@version}"
        end

        def _path
          "#{CLUSTERS_PATH}/#{@name}"
        end

        def _put_cluster(dic, params = nil)
          cluster = _put('', ApiCluster, false, dic, params)
          _update(cluster)
        end

        def get_service_types
          resp = @_resource_root.get(_path + '/serviceTypes')
          resp[ApiList::LIST_KEY]
        end

        def get_commands(view = nil)
          _get('commands', ApiCommand, true, view && { 'view' => view } || nil)
        end

        def rename(newname)
          dic = to_json_dict()
          if @_resource_root.version < 6
            dic['name'] = newname
          else
            dic['displayName'] = newname
          end
          _put_cluster(dic)
        end

        def update_cdh_version(new_cdh_version)
          dic = to_json_dict()
          dic['fullVersion'] = new_cdh_version
          _put_cluster(dic)
        end

        services_create_service = instance_method(:create_service)
        define_method(:create_service) do |name, service_type|
          services_create_service.bind(self).(name, service_type, @name)
        end

        services_delete_service = instance_method(:delete_service)
        define_method(:delete_service) do |name|
          services_delete_service.bind(self).(name, @name)
        end

        services_get_service = instance_method(:get_service)
        define_method(:get_service) do |name|
          services_get_service.bind(self).(name, @name)
        end

        services_get_all_services = instance_method(:get_all_services)
        define_method(:get_all_services) do |view = nil|
          services_get_all_services.bind(self).(@name, view)
        end

        parcels_get_parcel = instance_method(:get_parcel)
        define_method(:get_parcel) do |product, version|
          parcels_get_parcel.bind(self).(product, version, @name)
        end

        parcels_get_all_parcels = instance_method(:get_all_parcels)
        define_method(:get_all_parcels) do |view = nil|
          parcels_get_all_parcels.bind(self).(@name, view)
        end

        def list_hosts
          _get('hosts', ApiHostRef, true, nil, 3)
        end

        def remove_host(hostId)
          _delete('hosts/' + hostId, ApiHostRef, false, nil, 3)
        end

        def remove_all_hosts
          _delete('hosts', ApiHostRef, true, nil, 3)
        end

        def add_hosts(hostIds)
          hostRefList = []
          hostIds.each do |hostid|
            hostRefList << ApiHostRef.new(@_resource_root, hostid)
          end
          _post('hosts', ApiHostRef, true, hostRefList, nil, 3)
        end

        def start
          _cmd('start')
        end

        def stop
          _cmd('stop')
        end

        def restart(restart_only_stale_services = nil, redeploy_client_configuration = nil, restart_service_names = nil)
          if @_resource_root.version < 6
            _cmd('restart')
          else
            args = {}
            args['restartOnlyStaleServices'] = restart_only_stale_services
            args['redeployClientConfiguration'] = redeploy_client_configuration
            if @_resource_root.version >= 11
              args['restartServiceNames'] = restart_service_names
            end
            _cmd('restart', args, nil, 6)
          end
        end

        def deploy_client_config
          _cmd('deployClientConfig')
        end

        def deploy_cluster_client_config(hostIds = [])
          _cmd('deployClusterClientConfig', hostIds, nil, 7)
        end

        def enter_maintenance_mode
          cmd = _cmd('enterMaintenanceMode')
          if cmd.success
            _update(get_cluster(@name))
          end
          cmd
        end

        def exit_maintenance_mode
          cmd = _cmd('exitMaintenanceMode')
          if cmd.success
            _update(get_cluster(@name))
          end
          cmd
        end

        # TODO: get_all_host_templates
        # TODO: get_host_template
        # TODO: create_host_template
        # TODO: delete_host_template

        def rolling_restart(slave_batch_size = nil, slave_fail_count_threshold = nil, sleep_seconds = nil, stale_configs_only = nil, unupgraded_only = nil, roles_to_include = nil, restart_service_names = nil)
          args = {}
          args['slaveBatchSize'] = slave_batch_size if slave_batch_size
          args['slaveFailCountThreshold'] = slave_fail_count_threshold if slave_fail_count_threshold
          args['sleepSeconds'] = sleep_seconds if sleep_seconds
          args['staleConfigsOnly'] = stale_configs_only if stale_configs_only
          args['unUpgradedOnly'] = unupgraded_only if unupgraded_only
          args['rolesToInclude'] = roles_to_include if roles_to_include
          args['restartServiceNames'] = restart_service_names if restart_service_names

          _cmd('rollingRestart', args, nil, 4)
        end

        # TODO: rolling_upgrade

        def auto_assign_roles
          _put('autoAssignRoles', nil, false, nil, nil, 6)
        end

        def auto_configure
          _put('autoConfigure', nil, false, nil, nil, 6)
        end

        def first_run
          _cmd('firstRun', nil, nil, 7)
        end

        # TODO: upgrade_cdh

        def configure_for_kerberos(datanode_transceiver_port = nil, datanode_web_port = nil)
          args = {}
          args['datanodeTransceiverPort'] = datanode_transceiver_port if datanode_transceiver_port
          args['datanodeWebPort'] = datanode_web_port if datanode_web_port

          _cmd('configureForKerberos', args, nil, 11)
        end

        def export(export_auto_config = false)
          _get('export', ApiClusterTemplate, false, { 'exportAutoConfig' => export_auto_config }, 12)
        end

        def pools_refresh
          _cmd('poolsRefresh', nil, nil, 6)
        end

        def list_dfs_services(view = nil)
          if view
            @_resource_root.get("#{_path()}/dfsServices?view=#{view}")
          else
            @_resource_root.get("#{_path()}/dfsServices")
          end
        end
      end
    end
  end
end
