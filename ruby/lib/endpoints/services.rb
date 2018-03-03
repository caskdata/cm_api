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

require 'date'
require 'time'

require_relative 'types'
require_relative 'roles'
require_relative 'role_config_groups'

module CmApi
  module Endpoints
    # Module for services-related methods and types
    module Services
      include ::CmApi::Endpoints::Types
      include ::CmApi::Endpoints::Roles
      include ::CmApi::Endpoints::RoleConfigGroups

      SERVICES_PATH = '/clusters/%s/services'.freeze
      SERVICE_PATH = '/clusters/%s/services/%s'.freeze
      ROLETYPES_CFG_KEY = 'roleTypeConfigs'.freeze

      def create_service(name, service_type, cluster_name = 'default')
        apiservice = ApiService.new(self, name, service_type)
        # Ruby port note: as this module is included into other BaseApiResource objects (ApiCluster), we call
        # the :get/:post method on the @_resource_root instance variable object (ApiResource). In other modules (Clusters)
        # we call the method directly as they are included into an ApiResource object
        call_resource(@_resource_root.method(:post), format(SERVICES_PATH, cluster_name), ApiService, true, [apiservice])[0]
      end

      def get_service(name, cluster_name = 'default')
        _get_service(format('%s/%s', format(SERVICES_PATH, cluster_name), name))
      end

      def _get_service(path)
        call_resource(@_resource_root.method(:get), path, ApiService)
      end

      def get_all_services(cluster_name = 'default', view = nil)
        call_resource(@_resource_root.method(:get), format(SERVICES_PATH, cluster_name), ApiService, true, nil, view && { 'view' => view } || nil)
      end

      def delete_service(name, cluster_name = 'default')
        call_resource(@_resource_root.method(:delete), format('%s/%s', format(SERVICES_PATH, cluster_name), name), ApiService)
      end

      # Model for a service
      class ApiService < BaseApiResource
        include ::CmApi::Endpoints::Roles
        include ::CmApi::Endpoints::RoleConfigGroups
        @_ATTRIBUTES = {
          'name' => nil,
          'type' => nil,
          'displayName' => nil,
          'serviceState' => ROAttr.new,
          'healthSummary' => ROAttr.new,
          'healthChecks' => ROAttr.new,
          'clusterRef' => ROAttr.new(ApiClusterRef),
          'configStale' => ROAttr.new,
          'configStalenessStatus' => ROAttr.new,
          'clientConfigStalenessStatus' => ROAttr.new,
          'serviceUrl' => ROAttr.new,
          'roleInstancesUrl' => ROAttr.new,
          'maintenanceMode' => ROAttr.new,
          'maintenanceOwners' => ROAttr.new,
          'entityStatus' => ROAttr.new
        }

        def initialize(resource_root, name = nil, type = nil)
          super(resource_root, { name: name, type: type })
        end

        def to_s
          "<ApiService>: #{@name} (cluster: #{_get_cluster_name})"
        end

        def _get_cluster_name
          if instance_variable_get('@clusterRef') && @clusterRef
            return @clusterRef.clusterName
          end
        end

        def _path
          # This method assumes that lack of a cluster reference means that the
          # object refers to the Cloudera Management Services instance.
          return format(SERVICE_PATH, _get_cluster_name, @name) if _get_cluster_name
          '/cm/service'
        end

        def _role_cmd(cmd, roles, api_version=1)
          return _post("roleCommands/" + cmd, ApiBulkCommandList, false, roles, nil, api_version)
        end

        def _parse_svc_config(json_dic, view = nil)
          svc_config = ::CmApi::Endpoints::Types::json_to_config(json_dic, view == 'full')
          rt_configs = {}
          if json_dic.key?(ROLETYPES_CFG_KEY)
            json_dic[ROLETYPES_CFG_KEY].each do |rt_config, _v|
              rt_configs[rt_config['roleType']] = ::CmApi::Endpoints::Types::json_to_config(rt_config, view == 'full')
            end
          end
          return [ svc_config, rt_configs ]
        end

        def get_commands(view = nil)
          _get('commands', ApiCommand, true, nil, view && { 'view' => view } || nil)
        end

        def get_running_activities()
          query_activities()
        end

        def query_activities(query_str = nil)
          _get('activites', ApiActivity, true, query_str && { 'query' => query_str } || nil)
        end

        # TODO: get_impala_queries
        # TODO: cancel_impala_query
        # TODO: get_query_details
        # TODO: get_impala_query_attributes
        # TODO: create_impala_catalog_database
        # TODO: create_impala_user_dir
        # TODO: enable_llama_rm
        # TODO: disable_llama_rm
        # TODO: enable_llama_ha
        # TODO: disable_llama_ha

        def get_yarn_applications(start_time, end_time, filter_str = '', limit = 100, offset = 0)
          params = {
            'from' => start_time.iso8601(6),
            'to' => end_time.iso8601(6),
            'filter' => filter_str,
            'limit' => limit,
            'offset' => offset
          }
          _get('yarnApplications', ApiYarnApplicationResponse, false, params, 6)
        end

        def kill_yarn_application(application_id)
          _post(format('yarnApplications/%s/kill' % application_id), ApiYarnKillResponse, false, nil, nil, 6)
        end

        def get_yarn_application_attributes()
          _get('yarnApplications/attributes', ApiYarnApplicationAttribute, true, nil, 6)
        end

        def create_yarn_job_history_dir()
          _cmd('yarnCreateJobHistoryDirCommand', nil, nil, 6)
        end

        def create_yarn_node_manager_remote_app_log_dir()
          _cmd('yarnNodeManagerRemoteAppLogDirCommand', nil, nil, 6)
        end

        def collect_yarn_application_diagnostics(*application_ids)
          args = { 'applicationIds' => application_ids }
          _cmd('yarnApplicationDiagnosticsCollection', args, nil, 8)
        end

        def create_yarn_application_diagnostics_bundle(application_ids, ticket_number = nil, comments = nil)
          args = {
            'applicationIds' => application_ids,
            'ticketNumber' => ticket_number,
            'comments' => comments
          }
          _cmd('yarnApplicationDiagnosticsCollection', args, nil, 10)
        end

        def get_config(view = nil)
          path = _path() + '/config'
          resp = @_resource_root.get(path, view && {'view' => view} || nil)
          _parse_svc_config(resp, view)
        end

        def update_config(svc_config, rt_configs = nil)
          path = _path() + '/config'

          if svc_config
            data = config_to_api_list(svc_config)
          else
            data = {}
          end

          if rt_configs
            rt_list = []
            rt_configs.each do |rt, cfg|
              rt_data = config_to_api_list(cfg)
              rt_data['roleType'] = rt
              rt_list << rt_data
            end
            data[ROLETYPES_CFG_KEY] = rt_list
          end

          resp = @_resource_root.put(path, nil, data.to_json)
          _parse_svc_config(resp)
        end

        #  # !! TODO name collision?
        #  def create_role(role_name, role_type, host_id)
        #    create_role(@_resource_root, @name, role_type, role_name, host_id, _get_cluster_name)
        #  end

        # from roles.rb
        #def create_role(service_name, role_type, role_name, host_id, cluster_name = 'default')
        #  apirole = ApiRole.new(self, role_name, role_type, ApiHostRef.new(self, host_id))
        #  call_resource(@_resource_root.method(:post), _get_roles_path(cluster_name, service_name), ApiRole, true, [apirole])[0]
        #end

        ## solution, re-implement
        #def create_role(role_name, role_type, host_id)
        #  apirole = ApiRole.new(self, role_name, role_type, ApiHostRef.new(self, host_id))
        #  call_resource(@_resource_root.method(:post), _get_roles_path(_get_cluster_name, @name), ApiRole, true, [apirole])[0]
        #end

        # Ruby port note: wrapping method from another module
        roles_create_role = instance_method(:create_role)
        define_method(:create_role) do |role_name, role_type, host_id|
          roles_create_role.bind(self).(@name, role_type, role_name, host_id, _get_cluster_name)
        end

        #def delete_role(name)
        #  call_resource(@_resource_root.method(:delete), _get_role_path(_get_cluster_name, @name, name), ApiRole)
        #end

        # from roles.rb
        #def delete_role(service_name, name, cluster_name = 'default')
        #call_resource(@_resource_root.method(:delete), _get_role_path(cluster_name, service_name, name), ApiRole)
        #end

        # Ruby port note: wrapping method from another module
        roles_delete_role = instance_method(:delete_role)
        define_method(:delete_role) do |name|
          roles_delete_role.bind(self).(@name, name, _get_cluster_name)
        end

        # Ruby port note: wrapping method from another module
        roles_get_role = instance_method(:get_role)
        define_method(:get_role) do |name|
          roles_get_role.bind(self).(@name, name, _get_cluster_name)
        end

        # Ruby port note: wrapping method from another module
        roles_get_all_roles = instance_method(:get_all_roles)
        define_method(:get_all_roles) do |view = nil|
          roles_get_all_roles.bind(self).(@name, _get_cluster_name, view)
        end

        # Ruby port note: reimplementing a method from another module
        def get_roles_by_type(role_type, view = nil)
          roles = get_all_roles(view)
          roles.select { |r| r.type == role_type }
        end

        def get_role_types
          resp = @_resource_root.get(_path + '/roleTypes')
          resp[ApiList::LIST_KEY]
        end

        # Ruby port note: wrapping method from another module
        rcg_get_all_role_config_groups = instance_method(:get_all_role_config_groups)
        define_method(:get_all_role_config_groups) do
          rcg_get_all_role_config_groups.bind(self).(@name, _get_cluster_name)
        end

        # Ruby port note: wrapping method from another module
        rcg_get_role_config_group = instance_method(:get_role_config_group)
        define_method(:get_role_config_group) do |name|
          rcg_get_role_config_group.bind(self).(@name, name, _get_cluster_name)
        end

        # Ruby port note: wrapping method from another module
        rcg_create_role_config_group = instance_method(:create_role_config_group)
        define_method(:create_role_config_group) do |name, display_name, role_type|
          rcg_create_role_config_groups.bind(self).(@name, name, display_name, role_type, _get_cluster_name)
        end

        # Ruby port note: wrapping method from another module
        rcg_update_role_config_group = instance_method(:update_role_config_group)
        define_method(:update_role_config_group) do |name, apigroup|
          rcg_update_role_config_groups.bind(self).(@name, name, _get_cluster_name)
        end

        # Ruby port note: wrapping method from another module
        rcg_delete_role_config_group = instance_method(:delete_role_config_group)
        define_method(:delete_role_config_group) do |name|
          rcg_delete_role_config_groups.bind(self).(@name, name, _get_cluster_name)
        end


      #def get_role_config_group(service_name, name, cluster_name = 'default')
      #  _get_role_config_group(_get_role_config_group_path(cluster_name, service_name, name))
      #end

      #def _get_role_config_group(path)
      #  call_resource(@_resource_root.method(:get), path, ApiRoleConfigGroup, false, nil, nil, 3)
      #end

        # TODO: get_metrics

        def start
          _cmd('start')
        end

        def stop
          _cmd('stop')
        end

        def restart
          _cmd('restart')
        end

        def start_roles(role_names)
          _role_cmd('start', role_names)
        end

        def stop_roles(role_names)
          _role_cmd('stop', role_names)
        end

        def restart_roles(role_names)
          _role_cmd('restart', role_names)
        end

        def bootstrap_hdfs_stand_by(role_names)
          _role_cmd('hdfsBootstrapStandBy', role_names)
        end

        def finalize_metadata_upgrade(role_names)
          _role_cmd('hdfsFinalizeMetadataUpgrade', role_names, 3)
        end

        def create_hbase_root
          _cmd('hbaseCreateRoot')
        end

        def create_hdfs_tmp
          _cmd('hdfsCreateTmpDir')
        end

        def refresh(role_names)
          _role_cmd('refresh', role_names)
        end

        def decommission(role_names)
          _cmd('decommission', role_names)
        end

        def recommission(role_names)
          _cmd('recommission', role_names)
        end

        def deploy_client_config(role_names)
          _cmd('deployClientConfig', role_names)
        end

        def disable_hdfs_auto_failover(nameservice)
          _cmd('hdfsDisableAutoFailover', nameservice)
        end

        # TODO: disable_hdfs_ha
        # TODO: enable_hdfs_auto_failover
        # TODO: enable_hdfs_ha
        # TODO: enable_nn_ha
        # TODO: disable_nn_ha
        # TODO: enable_jt_ha
        # TODO: disable_jt_ha
        # TODO: enable_rm_ha
        # TODO: disable_rm_ha
        # TODO: enable_oozie_ha
        # TODO: disable_oozie_ha
        # TODO: failover_hdfs

        def format_hdfs(namenodes)
          _role_cmd('hdfsFormat', namenodes)
        end

        def init_hdfs_auto_failover(controllers)
          _role_cmd('hdfsInitializeAutoFailover', controllers)
        end

        def init_hdfs_shared_dir(namenodes)
          _role_cmd('hdfsInitializeSharedDir', namenodes)
        end

        def roll_edits_hdfs(nameservice = nil)
          args = {}
          if nameservice
            args['nameservice'] = nameservice
          end

          _cmd('hdfsRollEdits', args)
        end

        def upgrade_hdfs_metadata
          _cmd('hdfsUpgradeMetadata', nil, nil, 6)
        end

        def upgrade_hbase
          _cmd('hbaseUpgrade', nil, nil, 6)
        end

        def create_sqoop_user_dir
          _cmd('createSqoopUserDir', nil, nil, 4)
        end

        def create_sqoop_database_tables
          _cmd('sqoopCreateDatabaseTables', nil, nil, 10)
        end

        def upgrade_sqoop_db
          _cmd('sqoopUpgradeDb', nil, nil, 6)
        end

        def upgrade_hive_metastore
          _cmd('hiveUpgradeMetastore', nil, nil, 6)
        end

        def cleanup_zookeeper(servers)
          if servers
            _role_cmd('zooKeeperCleanup', servers)
          else
            _cmd('zooKeeperCleanup')
          end
        end

        def init_zookeeper(servers)
          if servers
            _role_cmd('zooKeeperInit', servers)
          else
            _cmd('zooKeeperInit')
          end
        end

        def sync_hue_db(servers)
          actual_version = @_resource_root.version
          if actual_version < 10
            _role_cmd('hueSyncDb', servers)
          end
          _cmd('hueSyncDb', nil, nil, 10)
        end

        def dump_hue_db
          _cmd('hueDumpDb', nil, nil, 10)
        end

        def load_hue_db
          _cmd('hueLoadDb', nil, nil, 10)
        end

        def lsof(rolenames)
          _role_cmd('lsof', rolenames)
        end

        def jstack(rolenames)
          _role_cmd('jstack', rolenames)
        end

        def jmap_histo(rolenames)
          _role_cmd('jmapHisto', rolenames)
        end

        def jmap_dump(rolenames)
          _role_cmd('jmapDump', rolenames)
        end

        def enter_maintenance_mode
          cmd = _cmd('enterMaintenanceMode')
          if cmd.success
            _update(_get_service(_path))
          end
          cmd
        end

        def exit_maintenance_mode
          cmd = _cmd('exitMaintenanceMode')
          if cmd.success
            _update(_get_service(_path))
          end
          cmd
        end

        def rolling_restart(slave_batch_size = nil, slave_fail_count_threshold = nil, sleep_seconds = nil, stale_configs_only = nil, unupgraded_only = nil, restart_role_types = nil, restart_role_names = nil)
          args = {}
          args['slaveBatchSize'] = slave_batch_size if slave_batch_size
          args['slaveFailCountThreshold'] = slave_fail_count_threshold if slave_fail_count_threshold
          args['sleepSeconds'] = sleep_seconds if sleep_seconds
          args['staleConfigsOnly'] = stale_configs_only if stale_configs_only
          args['unUpgradedOnly'] = unupgraded_only if unupgraded_only
          args['restartRoleTypes'] = restart_role_types if restart_role_types
          args['restartRoleNames'] = restart_role_names if restart_role_names

          _cmd('rollingRestart', args)
        end

        # TODO: create_replication_schedule
        # TODO: get_replication_schedules
        # TODO: get_replication_schedule
        # TODO: delete_replication_schedule
        # TODO: update_replication_schedule
        # TODO: get_replication_command_history
        # TODO: trigger_replication_schedule
        # TODO: create_snapshot_policy
        # TODO: get_snapshot_policies
        # TODO: get_snapshot_policy
        # TODO: delete_snapshot_policy
        # TODO: update_snapshot_policy
        # TODO: get_snapshot_command_history

        def install_oozie_sharelib
          _cmd('installOozieShareLib', nil, nil, 3)
        end

        def create_oozie_embedded_database
          _cmd('oozieCreateEmbeddedDatabase', nil, nil, 10)
        end

        def create_oozie_db
          _cmd('createOozieDb', nil, nil, 2)
        end

        def upgrade_oozie_db
          _cmd('oozieUpgradeDb', nil, nil, 6)
        end

        def init_solr
          _cmd('initSolr', nil, nil, 4)
        end

        def create_solr_hdfs_home_dir
          _cmd('createSolrHdfsHomeDir', nil, nil, 4)
        end

        def create_hive_metastore_tables
          _cmd('hiveCreateMetastoreDatabaseTables', nil, nil, 3)
        end

        def create_hive_warehouse
          _cmd('hiveCreateHiveWarehouse')
        end

        def create_hive_userdir
          _cmd('hiveCreateHiveUserDir')
        end

        def create_hive_metastore_database
          _cmd('hiveCreateMetastoreDatabase', nil, nil, 4)
        end

        def create_sentry_database
          _cmd('sentryCreateDatabase', nil, nil, 7)
        end

        def create_sentry_database_tables
          _cmd('sentryCreateDatabaseTables', nil, nil, 7)
        end

        def upgrade_sentry_database_tables
          _cmd('sentryUpgradeDatabaseTables', nil, nil, 8)
        end

        def update_metastore_namenodes
          _cmd('hiveUpdateMetastoreNamenodes', nil, nil, 4)
        end

        def import_mr_configs_into_yarn
          _cmd('importMrConfigsIntoYarn', nil, nil, 6)
        end

        def switch_to_mr2
          _cmd('switchToMr2', nil, nil, 6)
        end

        def finalize_rolling_upgrade
          _cmd('hdfsFinalizeRollingUpgrade', nil, nil, 8)
        end

        def role_command_by_name(command_name, role_names)
          _role_cmd(command_name, role_names, 6)
        end

        def service_command_by_name(command_name)
          _cmd(command_name, nil, nil, 6)
        end

        def list_commands_by_name
          _get('commandsByName', ApiCommandMetadata, true, nil, 6)
        end

        # Ruby port note: this is not defined in the python bindings, why not?
        def first_run
          _cmd('firstRun', nil, nil, 7)
        end

      end

      # Ruby port note
      # this should extend from ApiService. But it cant until BaseObject initialization can handle
      # a dynamic number of args.  need to look at using *args all the way up the chain.
      class ApiServiceSetupInfo < ApiService
        @_ATTRIBUTES = {
          'name' => nil,
          'type' => nil,
          'config' => Attr.new(ApiConfig),
          'roles' => Attr.new(ApiRole)
        }

        def initialize(resource_root, name = nil, type = nil, config = nil, roles = nil)
          # TODO: fix this ugly hack. super() doesn't work because it needs to pass in more stuff
          #class ApiServiceSetupInfo < BaseApiResource
          #super(nil, { name: name, type: type, config: config, roles: roles })
          grandparent = self.class.superclass.superclass
          meth = grandparent.instance_method(:initialize)
          meth.bind(self).call(nil, { name: name, type: type, config: config, roles: roles })
        end

        def set_config(config)
          @config ||= {}
          @config.update(config_to_api_list(config))
        end


        # TODO: add_role_type_info
        # TODO: add_role_info

        def first_run
          _cmd('firstRun', nil, nil, 7)
        end
      end
    end
  end
end
